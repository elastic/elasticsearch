/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.grpc.protobuf.ProtoUtils;

public class DynamicGrpcClient {
    private final ManagedChannel channel;
    private final MethodDescriptor<Any, Any> sendMessageMethod;

    public DynamicGrpcClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.sendMessageMethod = MethodDescriptor.<Any, Any>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName("DynamicService", "SendMessage"))
            .setRequestMarshaller(ProtoUtils.marshaller(Any.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Any.getDefaultInstance()))
            .build();
    }

    public String sendMessage(String message) {
        StringValue stringValue = StringValue.newBuilder().setValue(message).build();
        Any request = Any.pack(stringValue);

        Any response;
        try {
            response = ClientCalls.blockingUnaryCall(channel, sendMessageMethod, CallOptions.DEFAULT, request);
            StringValue responseValue = response.unpack(StringValue.class);
            return responseValue.getValue();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void shutdown() {
        channel.shutdown();
    }

    public static void main(String[] args) {

        DynamicGrpcClient client = new DynamicGrpcClient("localhost", 9999);
//        client.
//        String response = client.sendMessage("Hello, dynamic message!");
//        System.out.println("Response: " + response);
//        client.shutdown();
    }
}
