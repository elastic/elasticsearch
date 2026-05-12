/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Cluster-scoped authorization step for attaching a datasource when {@linkplain PutDatasetAction putting a dataset}.
 * REST executes this action before {@link PutDatasetAction} so {@code global.datasource} applies via
 * {@link org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageDatasourcePrivileges}.
 */
public class AuthorizeDatasetDatasourceAction extends ActionType<AcknowledgedResponse> {

    public static final AuthorizeDatasetDatasourceAction INSTANCE = new AuthorizeDatasetDatasourceAction();
    public static final String NAME = EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME;

    private AuthorizeDatasetDatasourceAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements DataSourceRequestInfo {

        private final String dataSource;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String dataSource) {
            super(masterNodeTimeout, ackTimeout);
            this.dataSource = Objects.requireNonNull(dataSource);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dataSource = in.readString();
        }

        /** Builds an authorize request using timeouts and datasource name from a dataset put request. */
        public static Request forDatasetPut(PutDatasetAction.Request putDatasetRequest) {
            return new Request(putDatasetRequest.masterNodeTimeout(), putDatasetRequest.ackTimeout(), putDatasetRequest.dataSource());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(dataSource);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (Strings.hasText(dataSource) == false) {
                return addValidationError("data_source is missing or empty", null);
            }
            return null;
        }

        public String dataSource() {
            return dataSource;
        }

        @Override
        public String[] dataSourceNames() {
            return new String[] { dataSource };
        }

        @Override
        public String dataSourceClusterActionName() {
            return NAME;
        }

    }
}
