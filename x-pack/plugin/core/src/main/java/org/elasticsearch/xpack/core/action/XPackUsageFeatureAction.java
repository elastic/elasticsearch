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

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Arrays;
import java.util.List;

public class XPackUsageFeatureAction extends Action<XPackUsageFeatureResponse> {

    private static final String BASE_NAME = "cluster:monitor/xpack/usage/";

    public static final XPackUsageFeatureAction SECURITY = new XPackUsageFeatureAction(XPackField.SECURITY);
    public static final XPackUsageFeatureAction MONITORING = new XPackUsageFeatureAction(XPackField.MONITORING);
    public static final XPackUsageFeatureAction WATCHER = new XPackUsageFeatureAction(XPackField.WATCHER);
    public static final XPackUsageFeatureAction GRAPH = new XPackUsageFeatureAction(XPackField.GRAPH);
    public static final XPackUsageFeatureAction MACHINE_LEARNING = new XPackUsageFeatureAction(XPackField.MACHINE_LEARNING);
    public static final XPackUsageFeatureAction LOGSTASH = new XPackUsageFeatureAction(XPackField.LOGSTASH);
    public static final XPackUsageFeatureAction SQL = new XPackUsageFeatureAction(XPackField.SQL);
    public static final XPackUsageFeatureAction ROLLUP = new XPackUsageFeatureAction(XPackField.ROLLUP);
    public static final XPackUsageFeatureAction INDEX_LIFECYCLE = new XPackUsageFeatureAction(XPackField.INDEX_LIFECYCLE);
    public static final XPackUsageFeatureAction CCR = new XPackUsageFeatureAction(XPackField.CCR);
    public static final XPackUsageFeatureAction DATA_FRAME = new XPackUsageFeatureAction(XPackField.DATA_FRAME);

    public static final List<XPackUsageFeatureAction> ALL = Arrays.asList(
        SECURITY, MONITORING, WATCHER, GRAPH, MACHINE_LEARNING, LOGSTASH, SQL, ROLLUP, INDEX_LIFECYCLE, CCR, DATA_FRAME
    );

    private XPackUsageFeatureAction(String name) {
        super(BASE_NAME + name);
    }

    @Override
    public XPackUsageFeatureResponse newResponse() {
        return new XPackUsageFeatureResponse();
    }
}
