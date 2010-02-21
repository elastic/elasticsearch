/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaDataService;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.Actions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportPutMappingAction extends TransportMasterNodeOperationAction<PutMappingRequest, PutMappingResponse> {

    private final MetaDataService metaDataService;

    private final TransportCreateIndexAction createIndexAction;

    private final boolean autoCreateIndex;

    @Inject public TransportPutMappingAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                             ThreadPool threadPool, MetaDataService metaDataService, TransportCreateIndexAction createIndexAction) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataService = metaDataService;
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = settings.getAsBoolean("action.autoCreateIndex", true);
    }


    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.Mapping.PUT;
    }

    @Override protected PutMappingRequest newRequest() {
        return new PutMappingRequest();
    }

    @Override protected PutMappingResponse newResponse() {
        return new PutMappingResponse();
    }

    @Override protected PutMappingResponse masterOperation(PutMappingRequest request) throws ElasticSearchException {
        final String[] indices = processIndices(clusterService.state(), request.indices());
        MetaDataService.PutMappingResult result = metaDataService.putMapping(indices, request.type(), request.mappingSource(), request.ignoreDuplicates(), request.timeout());
        return new PutMappingResponse(result.acknowledged());
    }

    @Override protected void doExecute(final PutMappingRequest request, final ActionListener<PutMappingResponse> listener) {
        final String[] indices = processIndices(clusterService.state(), request.indices());
        if (autoCreateIndex) {
            final CountDownLatch latch = new CountDownLatch(indices.length);
            for (String index : indices) {
                if (!clusterService.state().metaData().hasIndex(index)) {
                    createIndexAction.execute(new CreateIndexRequest(index), new ActionListener<CreateIndexResponse>() {
                        @Override public void onResponse(CreateIndexResponse response) {
                            latch.countDown();
                        }

                        @Override public void onFailure(Throwable e) {
                            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                                latch.countDown();
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    });
                } else {
                    latch.countDown();
                }
            }
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        super.doExecute(request, listener);
    }
}