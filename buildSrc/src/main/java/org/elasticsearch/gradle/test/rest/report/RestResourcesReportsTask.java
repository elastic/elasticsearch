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

import groovy.lang.Closure;
import groovy.lang.DelegatesTo;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.reporting.Reporting;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.TaskAction;
import org.gradle.util.ClosureBackedAction;

import javax.inject.Inject;

/**
 * The task which executes the reports.<br>
 * For example:
 * <pre>
 *  Provider&lt;Task&gt; reportTask = p.tasks.register('restTestsSourceReport', RestResourcesReportsTask) {
 *         reports {
 *           source_report {
 *             destination new File("/tmp/report.txt")
 *             projects ':modules', ':plugins', ':rest-api-spec', ':x-pack:plugin'  //includes subprojects
 *             excludes '&#42;&#42;/watcher/qa/&#42;&#42;/mustache/&#42;&#42;', '&#42;&#42;/watcher/qa/&#42;&#42;/painless/&#42;&#42;'
 *             outputs.upToDateWhen { false }
 *             enabled true
 *           }
 *         }
 *       }
 * </pre>
 */
public class RestResourcesReportsTask extends DefaultTask implements Reporting<RestResourcesReportsContainer> {

    private final RestResourcesReportsContainer reports;

    public RestResourcesReportsTask() {
        this.reports = getObjectFactory().newInstance(RestResourcesReportsContainer.class, this);
    }

    @Inject
    protected ObjectFactory getObjectFactory() {
        throw new UnsupportedOperationException();
    }

    @TaskAction
    public void run() {
        reports.getReportNames().forEach(name -> {
            RestResourceReport report = reports.getReport(name);
            if (report.isEnabled()) {
                report.appendToReport(getProject(), getLogger());
            }
        });
    }

    @Override
    @Nested
    public final RestResourcesReportsContainer getReports() {
        return reports;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public RestResourcesReportsContainer reports(
        @DelegatesTo(value = RestResourcesReportsContainer.class, strategy = Closure.DELEGATE_FIRST) Closure closure
    ) {
        return reports(new ClosureBackedAction<>(closure));
    }

    @Override
    public RestResourcesReportsContainer reports(Action<? super RestResourcesReportsContainer> configureAction) {
        configureAction.execute(reports);
        return reports;
    }
}
