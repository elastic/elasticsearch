/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ai21.Ai21Service;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchService;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockMockRequestSender;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxService;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields;
import org.elasticsearch.xpack.inference.services.llama.LlamaService;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.common.settings.Settings.EMPTY;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithSettings;

/**
 * Per-service, per-task-type descriptors for {@link TransportUpdateInferenceModelActionSecretRotationTests}.
 *
 * <p>When to add a new entry here:
 * <ul>
 *   <li>A <em>new</em> service is added that has secrets.</li>
 *   <li>A <em>new task type</em> is added to an existing service — each task type has its own {@code ServiceSettings}
 *       class and therefore its own update parser, so a new task type needs its own row.</li>
 * </ul>
 *
 * <p>When you do <em>not</em> need to add an entry: converting an already-listed service to
 * {@link org.elasticsearch.xcontent.ObjectParser}-based settings.  The existing rows already exercise whatever
 * parsing implementation that settings class uses at test time, so if you forget to allow secret fields in the
 * new parser the existing rows will fail.
 *
 * <p>The failure mode being guarded: secret fields (e.g. {@code api_key}, {@code access_key}) arrive
 * inside the {@code service_settings} JSON block of an update request.  A strict {@code ObjectParser}
 * that has not declared them via {@code allowSecretFields} rejects them as unknown fields, breaking
 * secret rotation.
 */
public class SecretRotationTestCases {

    private static final String INITIAL_API_KEY = "initial-api-key";
    private static final String ROTATED_API_KEY = "rotated-api-key";
    private static final String INITIAL_ACCESS_KEY = "initial-access-key";
    private static final String ROTATED_ACCESS_KEY = "rotated-access-key";
    private static final String INITIAL_SECRET_KEY = "initial-secret-key";
    private static final String ROTATED_SECRET_KEY = "rotated-secret-key";

    private static List<TransportUpdateInferenceModelActionSecretRotationTests.TestCase> cohereCases() {
        BiFunction<ThreadPool, HttpClientManager, SenderService<?>> factory = (tp, cm) -> new CohereService(
            HttpRequestSenderTests.createSenderFactory(tp, cm),
            createWithEmptySettings(tp),
            mockClusterServiceEmpty()
        );

        var cases = new ArrayList<TransportUpdateInferenceModelActionSecretRotationTests.TestCase>();

        cases.add(
            new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                CohereService.NAME,
                TaskType.TEXT_EMBEDDING,
                factory,
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "embed-v4.0",
                        ServiceFields.EMBEDDING_TYPE,
                        CohereEmbeddingType.FLOAT.toString(),
                        CohereCommonServiceSettings.API_VERSION,
                        CohereCommonServiceSettings.CohereApiVersion.V2.toString()
                    )
                ),
                new HashMap<>(),
                new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, INITIAL_API_KEY)),
                new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, ROTATED_API_KEY))
            )
        );

        cases.add(
            new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                CohereService.NAME,
                TaskType.COMPLETION,
                factory,
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "command-r-plus",
                        CohereCommonServiceSettings.API_VERSION,
                        CohereCommonServiceSettings.CohereApiVersion.V1.toString()
                    )
                ),
                new HashMap<>(),
                new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, INITIAL_API_KEY)),
                new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, ROTATED_API_KEY))
            )
        );

        cases.add(
            new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                CohereService.NAME,
                TaskType.RERANK,
                factory,
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        "rerank-english-v3.0",
                        CohereCommonServiceSettings.API_VERSION,
                        CohereCommonServiceSettings.CohereApiVersion.V2.toString()
                    )
                ),
                new HashMap<>(),
                new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, INITIAL_API_KEY)),
                new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, ROTATED_API_KEY))
            )
        );

        return cases;
    }

    private static List<TransportUpdateInferenceModelActionSecretRotationTests.TestCase> amazonBedrockCases() {
        BiFunction<ThreadPool, HttpClientManager, SenderService<?>> factory = (tp, cm) -> {
            var bedrockFactory = new AmazonBedrockMockRequestSender.Factory(createWithSettings(tp, EMPTY), mockClusterServiceEmpty());
            return new AmazonBedrockService(
                HttpRequestSenderTests.createSenderFactory(tp, cm),
                bedrockFactory,
                createWithEmptySettings(tp),
                mockClusterServiceEmpty()
            );
        };

        // Two secret fields: access_key and secret_key
        var region = "us-east-1";
        var initialSecrets = new HashMap<String, Object>(
            Map.of(AmazonBedrockConstants.ACCESS_KEY_FIELD, INITIAL_ACCESS_KEY, AmazonBedrockConstants.SECRET_KEY_FIELD, INITIAL_SECRET_KEY)
        );
        var rotatedSecrets = new HashMap<String, Object>(
            Map.of(AmazonBedrockConstants.ACCESS_KEY_FIELD, ROTATED_ACCESS_KEY, AmazonBedrockConstants.SECRET_KEY_FIELD, ROTATED_SECRET_KEY)
        );

        var cases = new ArrayList<TransportUpdateInferenceModelActionSecretRotationTests.TestCase>();

        cases.add(
            new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                AmazonBedrockService.NAME,
                TaskType.TEXT_EMBEDDING,
                factory,
                new HashMap<>(
                    Map.of(
                        AmazonBedrockConstants.REGION_FIELD,
                        region,
                        AmazonBedrockConstants.MODEL_FIELD,
                        "amazon.titan-embed-text-v2:0",
                        AmazonBedrockConstants.PROVIDER_FIELD,
                        "amazontitan"
                    )
                ),
                new HashMap<>(),
                new HashMap<>(initialSecrets),
                new HashMap<>(rotatedSecrets)
            )
        );

        cases.add(
            new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                AmazonBedrockService.NAME,
                TaskType.COMPLETION,
                factory,
                new HashMap<>(
                    Map.of(
                        AmazonBedrockConstants.REGION_FIELD,
                        region,
                        AmazonBedrockConstants.MODEL_FIELD,
                        "anthropic.claude-3-sonnet-20240229-v1:0",
                        AmazonBedrockConstants.PROVIDER_FIELD,
                        "anthropic"
                    )
                ),
                new HashMap<>(),
                new HashMap<>(initialSecrets),
                new HashMap<>(rotatedSecrets)
            )
        );

        cases.add(
            new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                AmazonBedrockService.NAME,
                TaskType.CHAT_COMPLETION,
                factory,
                new HashMap<>(
                    Map.of(
                        AmazonBedrockConstants.REGION_FIELD,
                        region,
                        AmazonBedrockConstants.MODEL_FIELD,
                        "amazon.nova-lite-v1:0",
                        AmazonBedrockConstants.PROVIDER_FIELD,
                        "amazontitan"
                    )
                ),
                new HashMap<>(),
                new HashMap<>(initialSecrets),
                new HashMap<>(rotatedSecrets)
            )
        );

        return cases;
    }

    private static List<TransportUpdateInferenceModelActionSecretRotationTests.TestCase> alibabaCloudSearchCases() {
        BiFunction<ThreadPool, HttpClientManager, SenderService<?>> factory = (tp, cm) -> new AlibabaCloudSearchService(
            HttpRequestSenderTests.createSenderFactory(tp, cm),
            createWithEmptySettings(tp),
            mockClusterServiceEmpty()
        );

        // Common service settings for all Alibaba task types
        var baseServiceSettings = Map.of(
            AlibabaCloudSearchServiceSettings.SERVICE_ID,
            "ops-text-embedding-001",
            AlibabaCloudSearchServiceSettings.HOST,
            "dashscope.aliyuncs.com",
            AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
            "default"
        );

        var cases = new ArrayList<TransportUpdateInferenceModelActionSecretRotationTests.TestCase>();

        for (var taskType : List.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING, TaskType.RERANK, TaskType.COMPLETION)) {
            cases.add(
                new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                    AlibabaCloudSearchService.NAME,
                    taskType,
                    factory,
                    new HashMap<>(baseServiceSettings),
                    new HashMap<>(),
                    new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, INITIAL_API_KEY)),
                    new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, ROTATED_API_KEY))
                )
            );
        }

        return cases;
    }

    private static List<TransportUpdateInferenceModelActionSecretRotationTests.TestCase> ibmWatsonxCases() {
        BiFunction<ThreadPool, HttpClientManager, SenderService<?>> factory = (tp, cm) -> new IbmWatsonxService(
            HttpRequestSenderTests.createSenderFactory(tp, cm),
            createWithEmptySettings(tp),
            mockClusterServiceEmpty()
        );

        var baseServiceSettings = Map.of(
            ServiceFields.URL,
            "https://us-south.ml.cloud.ibm.com",
            ServiceFields.MODEL_ID,
            "ibm/granite-13b-instruct-v2",
            IbmWatsonxServiceFields.PROJECT_ID,
            "my-project-id",
            IbmWatsonxServiceFields.API_VERSION,
            "2024-03-14"
        );

        var cases = new ArrayList<TransportUpdateInferenceModelActionSecretRotationTests.TestCase>();

        for (var taskType : List.of(TaskType.TEXT_EMBEDDING, TaskType.RERANK, TaskType.COMPLETION, TaskType.CHAT_COMPLETION)) {
            cases.add(
                new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                    IbmWatsonxService.NAME,
                    taskType,
                    factory,
                    new HashMap<>(baseServiceSettings),
                    new HashMap<>(),
                    new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, INITIAL_API_KEY)),
                    new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, ROTATED_API_KEY))
                )
            );
        }

        return cases;
    }

    private static List<TransportUpdateInferenceModelActionSecretRotationTests.TestCase> ai21Cases() {
        BiFunction<ThreadPool, HttpClientManager, SenderService<?>> factory = (tp, cm) -> new Ai21Service(
            HttpRequestSenderTests.createSenderFactory(tp, cm),
            createWithEmptySettings(tp),
            mockClusterServiceEmpty()
        );

        var cases = new ArrayList<TransportUpdateInferenceModelActionSecretRotationTests.TestCase>();

        for (var taskType : List.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION)) {
            cases.add(
                new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                    Ai21Service.NAME,
                    taskType,
                    factory,
                    new HashMap<>(Map.of(ServiceFields.MODEL_ID, "jamba-1.5-mini")),
                    new HashMap<>(),
                    new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, INITIAL_API_KEY)),
                    new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, ROTATED_API_KEY))
                )
            );
        }

        return cases;
    }

    private static List<TransportUpdateInferenceModelActionSecretRotationTests.TestCase> llamaCases() {
        BiFunction<ThreadPool, HttpClientManager, SenderService<?>> factory = (tp, cm) -> new LlamaService(
            HttpRequestSenderTests.createSenderFactory(tp, cm),
            createWithEmptySettings(tp),
            mockClusterServiceEmpty()
        );

        var baseServiceSettings = Map.of(ServiceFields.URL, "http://localhost:11434", ServiceFields.MODEL_ID, "llama3.2");

        var cases = new ArrayList<TransportUpdateInferenceModelActionSecretRotationTests.TestCase>();

        for (var taskType : List.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION, TaskType.CHAT_COMPLETION)) {
            cases.add(
                new TransportUpdateInferenceModelActionSecretRotationTests.TestCase(
                    LlamaService.NAME,
                    taskType,
                    factory,
                    new HashMap<>(baseServiceSettings),
                    new HashMap<>(),
                    new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, INITIAL_API_KEY)),
                    new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, ROTATED_API_KEY))
                )
            );
        }

        return cases;
    }

    /**
     * Returns all parameterized test cases.  Cases are ordered: Wave 1 (services with
     * {@code usesParserForServiceSettings() == true}) first, so regressions in the fix itself show up at
     * the top of the test run.
     */
    public static List<TransportUpdateInferenceModelActionSecretRotationTests.TestCase> all() {
        var cases = new ArrayList<TransportUpdateInferenceModelActionSecretRotationTests.TestCase>();
        cases.addAll(cohereCases());
        cases.addAll(amazonBedrockCases());
        cases.addAll(alibabaCloudSearchCases());
        cases.addAll(ibmWatsonxCases());
        cases.addAll(ai21Cases());
        cases.addAll(llamaCases());
        return cases;
    }
}
