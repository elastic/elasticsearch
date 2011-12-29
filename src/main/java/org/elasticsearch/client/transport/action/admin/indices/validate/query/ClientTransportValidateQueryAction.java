package org.elasticsearch.client.transport.action.admin.indices.validate.query;

import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.client.transport.action.support.BaseClientTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class ClientTransportValidateQueryAction extends BaseClientTransportAction<ValidateQueryRequest, ValidateQueryResponse> {

    @Inject
    public ClientTransportValidateQueryAction(Settings settings, TransportService transportService) {
        super(settings, transportService, ValidateQueryResponse.class);
    }

    @Override
    protected String action() {
        return TransportActions.Admin.Indices.VALIDATE_QUERY;
    }
}
