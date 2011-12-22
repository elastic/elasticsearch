package org.elasticsearch.client.transport.action.validate;

import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.validate.ValidateRequest;
import org.elasticsearch.action.validate.ValidateResponse;
import org.elasticsearch.client.transport.action.support.BaseClientTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class ClientTransportValidateAction extends BaseClientTransportAction<ValidateRequest, ValidateResponse> {

    @Inject
    public ClientTransportValidateAction(Settings settings, TransportService transportService) {
        super(settings, transportService, ValidateResponse.class);
    }

    @Override
    protected String action() {
        return TransportActions.VALIDATE;
    }
}
