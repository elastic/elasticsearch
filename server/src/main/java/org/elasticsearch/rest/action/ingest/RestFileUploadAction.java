/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
/*

  File Upload API.

  1. The client asks for an upload key

  POST /_upload/key
  {
    "filename": "myfile.pdf",
    "filetype": "application/pdf",
    "size": 12345
  }

  <key>

  2. The file gets uploaded

  POST /_upload/<key>
  <DATA>

  3. Then it's available to the attachment plugin via a classical call:

  PUT my-index-000001/_doc/my_id?pipeline=attachment
  {
    "data": "file:key",
  }


 */
package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestFileUploadAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "ingest_file_upload_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_upload/key"),    // returns a key
            new Route(POST, "/_upload/{key}"), // used to transfer the file
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // TZ implement the resumable download here
        return channel -> { channel.sendResponse(new RestResponse(RestStatus.OK, "text/plain", "Hello new API")); };
    }
}
