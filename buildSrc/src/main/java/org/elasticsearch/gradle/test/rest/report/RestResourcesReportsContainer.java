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
package org.elasticsearch.gradle.test.rest.report;

import org.gradle.api.Task;
import org.gradle.api.internal.CollectionCallbackActionDecorator;
import org.gradle.api.reporting.internal.TaskReportContainer;
import org.gradle.api.tasks.Internal;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;

/**
 * Reports container for Rest Resources.
 */
public class RestResourcesReportsContainer extends TaskReportContainer<RestResourceReport> {

    private final Map<String, Class<? extends RestResourceReport>> reports = Map.of("source_report", RestTestSourceReport.class);

    @Inject
    public RestResourcesReportsContainer(Task task, CollectionCallbackActionDecorator callbackActionDecorator) {
        super(RestResourceReport.class, task, callbackActionDecorator);
        reports.forEach((k, v) -> { add(v, k, task); });
    }

    RestResourceReport getReport(String reportName) {
        return getByName(reportName);
    }

    @Internal
    Set<String> getReportNames() {
        return reports.keySet();
    }
}
