package org.elasticsearch.graphql;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.rest.RestRequest;

final public class GraphqlService {
    ActionModule actionModule;
    GraphqlRestHandler restHandler;

    public GraphqlService(ActionModule actionModule) {
        this.actionModule = actionModule;
        restHandler = new GraphqlRestHandler();
        init();
    }

    void init () {
        actionModule.getRestController().registerHandler(RestRequest.Method.GET, "/graphql", restHandler);
    }
}
