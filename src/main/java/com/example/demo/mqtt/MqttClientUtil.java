package com.example.demo.mqtt;

import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.auth.Credentials;
import com.amazonaws.services.iot.client.auth.StaticCredentialsProvider;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.io.ClientBootstrap;
import software.amazon.awssdk.crt.io.EventLoopGroup;
import software.amazon.awssdk.crt.io.HostResolver;
import software.amazon.awssdk.crt.io.SocketOptions;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * create MQTT clients by AWS IoT Device SDK version
 * @author mambo
 */
@Slf4j
public class MqttClientUtil {
    private MqttClientUtil() {
    }

    static AWSIotMqttClient build(String clientEndpoint, String clientId, String accessKeyId, String secretKey, String sessionToken, String region) {
        StaticCredentialsProvider staticCredentialsProvider = new StaticCredentialsProvider(new Credentials(accessKeyId, secretKey, sessionToken));
        return new AWSIotMqttClient(clientEndpoint, clientId, staticCredentialsProvider, region);
    }

    static MqttClientConnection buildV2(String clientEndpoint, String clientId,
                                        String accessKeyId, String secretKey, String sessionToken, String region) throws UnsupportedEncodingException {
        try (EventLoopGroup eventLoopGroup = new EventLoopGroup(1);
             HostResolver resolver = new HostResolver(eventLoopGroup);
             ClientBootstrap clientBootstrap = new ClientBootstrap(eventLoopGroup, resolver);
             AwsIotMqttConnectionBuilder builder = AwsIotMqttConnectionBuilder.newDefaultBuilder()) {

            software.amazon.awssdk.crt.auth.credentials.StaticCredentialsProvider credentialsProvider =
                    new software.amazon.awssdk.crt.auth.credentials.StaticCredentialsProvider.StaticCredentialsProviderBuilder()
                            .withAccessKeyId(accessKeyId.getBytes(StandardCharsets.UTF_8))
                            .withSecretAccessKey(secretKey.getBytes(StandardCharsets.UTF_8))
                            .withSessionToken(sessionToken.getBytes(StandardCharsets.UTF_8))
                            .build();

            return builder
                    .withEndpoint(clientEndpoint)
                    .withClientId(clientId)
                    .withBootstrap(clientBootstrap)
                    .withSocketOptions(new SocketOptions())
                    .withWebsockets(true)
                    .withWebsocketCredentialsProvider(credentialsProvider)
                    .withWebsocketSigningRegion(region)
                    .withConnectionEventCallbacks(clientConnectionEvents())
                    .build();
        }
    }

    static MqttClientConnectionEvents clientConnectionEvents() {
        return new MqttClientConnectionEvents() {
            @Override
            public void onConnectionInterrupted(int errorCode) {
                if (errorCode != 0) {
                    log.info("Connection interrupted: " + errorCode + ": " + CRT.awsErrorString(errorCode));
                }
            }

            @Override
            public void onConnectionResumed(boolean sessionPresent) {
                log.info("Connection resumed: " + (sessionPresent ? "existing session" : "clean session"));
            }
        };
    }
}
