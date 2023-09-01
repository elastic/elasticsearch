/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.packageloader.action.OpenAIChatAction;
import org.elasticsearch.xpack.ml.packageloader.MachineLearningPackageLoader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class OpenAI extends TransportMasterNodeAction<OpenAIChatAction.Request, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(OpenAI.class);

    private final Client client;

    @Inject
    public OpenAI(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            OpenAIChatAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            OpenAIChatAction.Request::new,
            indexNameExpressionResolver,
            NodeAcknowledgedResponse::new,
            ThreadPool.Names.SAME
        );
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    protected ClusterBlockException checkBlock(OpenAIChatAction.Request request, ClusterState state) {
        return null;
    }

    @Override
    protected void masterOperation(
        Task task,
        OpenAIChatAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {

        threadPool.executor(MachineLearningPackageLoader.UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                invokePost();
            } catch (Exception e) {
                logger.error(format("Got an error while sending openai request"), e);
            }

            listener.onResponse(null);
        });

    }

    @SuppressWarnings("'java.lang.SecurityManager' is deprecated and marked for removal ")
    @SuppressForbidden(reason = "we need socket connection to talk to azure")
    public void invokePost() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        PrivilegedAction<Void> privHttpReader = () -> {
            try {
                HttpPost httpPost = getHttpPost();

                try (
                    CloseableHttpClient httpClient = HttpClients.createDefault();
                    CloseableHttpResponse response = httpClient.execute(httpPost);
                ) {

                    // Get HttpResponse Status
                    logger.info(format("Request status info %s", response.getStatusLine()));

                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        // return it as a String
                        String result = EntityUtils.toString(entity);
                        logger.info(format("Request response: %s", result));
                    }
                    EntityUtils.consume(entity);
                } catch (ParseException | IOException e) {
                    logger.error(format("Failure occurred while sending request"), e);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("Got an exception during getHttpPost call", e);
            }

            return null;
        };

        AccessController.doPrivileged(privHttpReader);
    }

    private static HttpPost getHttpPost() throws UnsupportedEncodingException {
        StringEntity stringEntity = getStringEntity();
        HttpPost httpPost = new HttpPost(
            "https://<resource name>.openai.azure.com/openai/deployments/jon-test-deployment/chat/completions?api-version=2023-05-15"
        );

        httpPost.setEntity(stringEntity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setHeader("api-key", "<api key>");
        return httpPost;
    }

    private static StringEntity getStringEntity() throws UnsupportedEncodingException {
        String body =
            "{\"messages\":[{\"role\": \"system\", \"content\": \"You are a helpful assistant.\"},{\"role\": \"user\", \"content\":"
                + " \"Does Azure OpenAI support customer managed keys?\"},{\"role\": \"assistant\", \"content\": \"Yes, customer managed"
                + " keys are supported by Azure OpenAI.\"},{\"role\": \"user\", \"content\": \"Do other Azure AI services support"
                + " this too?\"}]}";

        return new StringEntity(body);
    }

    private String prepareRequest() {
        var values = new HashMap<String, String>() {
            {
                put("Id", "12345");
                put("Customer", "Roger Moose");
                put("Quantity", "3");
                put("Price", "167.35");
            }
        };

        var objectMapper = new ObjectMapper();
        String requestBody = "";
        try {
            requestBody = objectMapper.writeValueAsString(values);
        } catch (JsonProcessingException e) {
            logger.error(format("Error while writing object to json"), e);
            e.printStackTrace();
        }
        return requestBody;
    }
}
