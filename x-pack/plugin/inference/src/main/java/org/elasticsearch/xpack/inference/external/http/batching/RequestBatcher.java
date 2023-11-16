/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.List;

/**
 * Things to think about:
 *  - A single sender will be used for ALL requests for a single service. For example the {@link org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserService}
 *  will only have one sender, therefore we will need to handle when the account information is separate (if there are different api keys
 *  then we can't combine the requests)
 *  - If the URLs are different then they need to be separate requests
 *  - What do we do if we drain 2 requests and they need to be separate? Just iterate of the requests?
 *      - What if we get a shutdown while we're doing that? No different then how things are currently, we can't cancel requests
 *      - If the current request fails for some reason, it should go through the onFailure logic and likely be retried but that'll just
 *          end back on the queue so that should be fine
 *  - We need something to track the parts of a batch
 *      - A request can have multiple input but they should all be associated with a single request when onResponse is called for that listener
 *      - We'll need to track a range within the array for that
 */
public interface RequestBatcher<T> {
    void add(RequestCreator<T> creator, List<String> input, ActionListener<HttpResult> listener);
}
