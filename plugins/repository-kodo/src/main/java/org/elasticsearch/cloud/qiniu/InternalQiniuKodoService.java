/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.cloud.qiniu;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

public class InternalQiniuKodoService extends AbstractLifecycleComponent implements QiniuKodoService {


    private Map<String, QiniuKodoClient> clients = new HashMap<>();

    public InternalQiniuKodoService(Settings settings) {
        super(settings);
    }

    @Override
    public synchronized QiniuKodoClient client(Settings repositorySettings, Integer maxRetries,
                                              boolean useThrottleRetries) {
        // TODO cache client

        QiniuKodoClient client = new QiniuKodoClient("","","","");
        return client;
    }

    String buildVal(Settings repositorySettings){
        return "";
    }


    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        for (QiniuKodoClient client : clients.values()) {
//            client.shutdown();
        }

        // Ensure that IdleConnectionReaper is shutdown
    }
}
