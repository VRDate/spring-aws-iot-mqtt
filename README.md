# AWS IoT MQTT
본 리파지토리는 Amazon Cognito에 인증된 크레덴셜을 사용하여 AWS IoT Service에 연결하는 예제입니다.
> 예제 코드는 AWS SDK를 사용하는 예시일 뿐 Amazon Cognito에 사용자 풀, 자격 증명 풀을 등록하고 관리하는 것을 설명하지 않습니다.

## AWS SDK v2
새로 작성된 [AWS SDK v2](https://github.com/aws/aws-sdk-java-v2)의 cognitoidentity, lambda 모듈을 사용합니다.

## MQTT Client of AWS IoT Device SDK

|MQTT Client|SDK|Version|
|---|---|---|
|AWSIotMqttClient|[AWS IoT Device SDK Java](https://github.com/aws/aws-iot-device-sdk-java)|1|
|MqttClientConnection|[AWS IoT Device SDK Java v2](https://github.com/aws/aws-iot-device-sdk-java-v2)|2|
