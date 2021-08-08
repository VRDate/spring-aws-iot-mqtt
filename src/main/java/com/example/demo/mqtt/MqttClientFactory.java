package com.example.demo.mqtt;

import com.amazonaws.services.iot.client.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.concurrent.ListenableFutureTask;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cognitoidentity.CognitoIdentityClient;
import software.amazon.awssdk.services.cognitoidentity.model.Credentials;
import software.amazon.awssdk.services.cognitoidentity.model.GetCredentialsForIdentityRequest;
import software.amazon.awssdk.services.cognitoidentity.model.GetIdRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
@Configuration
public class MqttClientFactory {
    private static final Gson gson = new GsonBuilder().create();
    private static final String COGNITO_IDENTITY_PROVIDER_URL = "cognito-idp.%s.amazonaws.com/%s";

    @Value("${aws-iot.cognito.identity-pool-id}")
    private String identityPoolId;
    @Value("${aws-iot.cognito.user-pool}")
    private String userPool;
    @Value("${aws-iot.lambda.auth.function-name}")
    private String authLambda;
    @Value("#{${aws-iot.lambda.auth.payload}}")
    private Map<String, Object> authLambdaPayload;
    @Value("${aws-iot.region}")
    private Region region;
    @Value("${aws-iot.client.endpoint}")
    private String clientEndpoint;
    @Value("${aws-iot.client.id}")
    private String clientId;
    @Value("${aws-iot.device.version}")
    private int version = 1;

    private MqttClientConnection connection;
    private AWSIotMqttClient awsIotMqttClient;

    @PostConstruct
    public void setup() {
        Credentials credentials = credentials();
        if (version == 2) {
            subscribeByDeviceV2(credentials);
        } else {
            subscribeByDevice(credentials);
        }
    }

    /**
     * Subscribe MQTT topic using AWS IoT Device SDK
     *
     * @param credentials Cognito Session Credentials
     */
    private void subscribeByDevice(Credentials credentials) {
        this.awsIotMqttClient = MqttClientUtil.build(clientEndpoint, clientId,
                credentials.accessKeyId(), credentials.secretKey(), credentials.sessionToken(), region.id());
        try {
            awsIotMqttClient.connect();
            awsIotMqttClient.subscribe(new SubscribeHandler("thing-update/#"));
        } catch (AWSIotException e) {
            log.error(e.getMessage());
        }
    }

    public static class SubscribeHandler extends AWSIotTopic {
        @Override
        public void onMessage(AWSIotMessage message) {
            log.info("topic:{}, qos:{}", message.getTopic(), message.getQos());
            log.info("===> {}", new String(message.getPayload(), StandardCharsets.UTF_8));
        }

        public SubscribeHandler(String topic) {
            super(topic, AWSIotQos.QOS0);
        }
    }

    /**
     * Subscribe MQTT topic using AWS IoT Device SDK v2
     *
     * @param credentials Cognito Session Credentials
     */
    @SuppressWarnings({"squid:S3740", "rawtypes", "unchecked"})
    private void subscribeByDeviceV2(Credentials credentials) {
        try {
            this.connection = MqttClientUtil.buildV2(clientEndpoint, clientId,
                    credentials.accessKeyId(), credentials.secretKey(), credentials.sessionToken(), region.id());
            CompletableFuture<Boolean> connectFuture = connection.connect();
            connectFuture.get();

            ListenableFutureTask futureTask = new ListenableFutureTask(() ->
                    connection.subscribe("thing-update/#", QualityOfService.AT_MOST_ONCE, handler -> {
                        String payload = new String(handler.getPayload(), StandardCharsets.UTF_8);
                        log.info("topic:{}, qos: {}", handler.getTopic(), handler.getQos());
                        log.trace("===> {}", payload);
                    }));

            futureTask.run();
        } catch (UnsupportedEncodingException | ExecutionException e) {
            log.error(e.getMessage());
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void close() throws AWSIotException {
        if (connection != null) connection.close();
        if (awsIotMqttClient != null && AWSIotConnectionStatus.DISCONNECTED.equals(awsIotMqttClient.getConnectionStatus())) {
            awsIotMqttClient.disconnect();
        }
    }

    /**
     * Request authentication with Lambda Authorizer to query a user's JWT token
     *
     * @param awsSessionCredentials Cognito Session Credentials
     * @param region Region
     * @return
     */
    @SuppressWarnings({"unchecked"})
    private String jwt(AwsSessionCredentials awsSessionCredentials, Region region) {
        // https://docs.aws.amazon.com/code-samples/latest/catalog/javav2-lambda-src-main-java-com-example-lambda-LambdaInvoke.java.html
        try (LambdaClient lambdaClient = LambdaClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsSessionCredentials))
                .region(region)
                .build()) {

            InvokeRequest invokeRequest = InvokeRequest.builder()
                    .functionName(authLambda)
                    .payload(SdkBytes.fromString(gson.toJson(authLambdaPayload), StandardCharsets.UTF_8))
                    .build();

            InvokeResponse invokeResponse = lambdaClient.invoke(invokeRequest);
            Map<String, Object> responsePayload = gson.fromJson(new String(invokeResponse.payload().asByteArray(), StandardCharsets.UTF_8),
                    new TypeToken<HashMap<String, Object>>() {
                    }.getType());

            return (String) ((Map<String, Object>) responsePayload.get("credentials")).get("token");
        }
    }

    private Credentials credentials() {
        AwsSessionCredentials sessionCredentials = baseCredentials(identityPoolId, region);
        String accessToken = jwt(sessionCredentials, region);
        return loginsCredentials(identityPoolId, region, userPool, accessToken);
    }

    /**
     * Cognito Identity Integration - SigV4
     *
     * @param identityPoolId Cognito Identity Pool Id
     * @param region         region of User Pool ID
     * @param userPoolId     User Pool ID
     * @param accessToken    jwt
     * @return
     */
    private static Credentials loginsCredentials(String identityPoolId, Region region, String userPoolId, String accessToken) {
        // NOTE: 자격 증명 공급자에 인증된 사용자의 JWT 토큰을 부여
        Map<String, String> logins = new HashMap<>();
        logins.put(String.format(COGNITO_IDENTITY_PROVIDER_URL, region.id(), userPoolId), accessToken);

        try (CognitoIdentityClient cognitoIdentityClient = CognitoIdentityClient.builder()
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .region(region)
                .build()) {

            GetIdRequest getIdRequest = GetIdRequest.builder()
                    .identityPoolId(identityPoolId)
                    .logins(logins)
                    .build();

            String identityId = cognitoIdentityClient.getId(getIdRequest).identityId();
            GetCredentialsForIdentityRequest getCredentialsForIdentityRequest = GetCredentialsForIdentityRequest.builder()
                    .identityId(identityId)
                    .logins(logins)
                    .build();

            return cognitoIdentityClient.getCredentialsForIdentity(getCredentialsForIdentityRequest).credentials();
        }
    }

    /**
     * Request Cognito Credentials - Temporary
     * @param identityPoolId Cognito Identity Pool Id
     * @param region Region
     * @return
     */
    private static AwsSessionCredentials baseCredentials(String identityPoolId, Region region) {
        try (CognitoIdentityClient cognitoIdentityClient = CognitoIdentityClient.builder()
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .region(region)
                .build()) {

            GetIdRequest getIdRequest = GetIdRequest.builder()
                    .identityPoolId(identityPoolId)
                    .build();

            String identityId = cognitoIdentityClient.getId(getIdRequest).identityId();
            GetCredentialsForIdentityRequest getCredentialsForIdentityRequest = GetCredentialsForIdentityRequest.builder()
                    .identityId(identityId)
                    .build();

            Credentials credentials = cognitoIdentityClient.getCredentialsForIdentity(getCredentialsForIdentityRequest).credentials();
            return AwsSessionCredentials.create(credentials.accessKeyId(), credentials.secretKey(), credentials.sessionToken());
        }
    }
}
