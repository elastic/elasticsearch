package org.elasticsearch.compat;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.plugins.RestRequestPlugin;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFactory;

public class CompatRestRequestPlugin implements RestRequestPlugin {
    @Override
    public RestRequestFactory getRestRequestFactory() {
        return new RestRequestFactory() {
            @Override
            public RestRequest createRestRequest(NamedXContentRegistry xContentRegistry, HttpRequest httpRequest, HttpChannel httpChannel) {
                RestRequest restRequest = RestRequest.request(xContentRegistry, httpRequest, httpChannel);
                return new CompatibleRestRequest(restRequest);
            }
        };
    }

    public static class CompatibleRestRequest extends RestRequest {

        // TODO this requires copying the content of original rest request. Isn't this against why multiple rest wrappers were disallowed?
        //https://github.com/elastic/elasticsearch/pull/21905/files#r90480951
        protected CompatibleRestRequest(RestRequest restRequest) {
            super(restRequest);
        }

        public Version getCompatibleApiVersion() {
            return Version.CURRENT;
        }


    }
}
