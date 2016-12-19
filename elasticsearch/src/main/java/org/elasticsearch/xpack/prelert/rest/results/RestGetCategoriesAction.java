/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.GetCategoriesDefinitionAction;
import org.elasticsearch.xpack.prelert.action.GetCategoriesDefinitionAction.Request;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.results.PageParams;

import java.io.IOException;

public class RestGetCategoriesAction extends BaseRestHandler {

    private final GetCategoriesDefinitionAction.TransportAction transportAction;

    @Inject
    public RestGetCategoriesAction(Settings settings, RestController controller,
                                   GetCategoriesDefinitionAction.TransportAction transportAction) {
        super(settings);
        this.transportAction = transportAction;
        controller.registerHandler(RestRequest.Method.GET,
                PrelertPlugin.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categorydefinitions/{"
                + Request.CATEGORY_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.GET,
                PrelertPlugin.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categorydefinitions", this);

        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categorydefinitions/{"
                + Request.CATEGORY_ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/results/categorydefinitions", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request;
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String categoryId = restRequest.param(Request.CATEGORY_ID.getPreferredName());
        BytesReference bodyBytes = restRequest.content();

        if (bodyBytes != null && bodyBytes.length() > 0) {
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            request = GetCategoriesDefinitionAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
            request.setCategoryId(categoryId);
        } else {

            request = new Request(jobId);
            if (!Strings.isNullOrEmpty(categoryId)) {
                request.setCategoryId(categoryId);
            }
            if (restRequest.hasParam(Request.FROM.getPreferredName())
                    || restRequest.hasParam(Request.SIZE.getPreferredName())
                    || Strings.isNullOrEmpty(categoryId)){

                request.setPageParams(new PageParams(
                        restRequest.paramAsInt(Request.FROM.getPreferredName(), 0),
                        restRequest.paramAsInt(Request.SIZE.getPreferredName(), 100)
                ));
            }
        }

        return channel -> transportAction.execute(request, new RestToXContentListener<>(channel));
    }

}
