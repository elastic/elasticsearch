/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index;

import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.jmx.JmxService;
import org.elasticsearch.jmx.MBean;
import org.elasticsearch.jmx.ManagedAttribute;

/**
 *
 */
@MBean(objectName = "", description = "")
public class IndexServiceManagement extends AbstractIndexComponent implements CloseableComponent {

    public static String buildIndexGroupName(Index index) {
        return "service=indices,index=" + index.name();
    }

    private final JmxService jmxService;

    private final IndexService indexService;

    @Inject
    public IndexServiceManagement(Index index, @IndexSettings Settings indexSettings, JmxService jmxService, IndexService indexService) {
        super(index, indexSettings);
        this.jmxService = jmxService;
        this.indexService = indexService;
    }

    public void close() {
        jmxService.unregisterGroup(buildIndexGroupName(indexService.index()));
    }

    @ManagedAttribute(description = "Index Name")
    public String getIndex() {
        return indexService.index().name();
    }
}
