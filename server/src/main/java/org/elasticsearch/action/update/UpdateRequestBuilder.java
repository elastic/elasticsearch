/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.update;

import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.Script;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

public class UpdateRequestBuilder extends InstanceShardOperationRequestBuilder<UpdateRequest, UpdateResponse, UpdateRequestBuilder>
    implements
        WriteRequestBuilder<UpdateRequestBuilder> {

    public UpdateRequestBuilder(ElasticsearchClient client, UpdateAction action) {
        super(client, action, new UpdateRequest());
    }

    public UpdateRequestBuilder(ElasticsearchClient client, UpdateAction action, String index, String id) {
        super(client, action, new UpdateRequest(index, id));
    }

    /**
     * Sets the id of the indexed document.
     */
    public UpdateRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public UpdateRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     * <p>
     * The script works with the variable <code>ctx</code>, which is bound to the entry,
     * e.g. <code>ctx._source.mycounter += 1</code>.
     *
     */
    public UpdateRequestBuilder setScript(Script script) {
        request.script(script);
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include
     *            An optional include (optionally wildcarded) pattern to filter
     *            the returned _source
     * @param exclude
     *            An optional exclude (optionally wildcarded) pattern to filter
     *            the returned _source
     */
    public UpdateRequestBuilder setFetchSource(@Nullable String include, @Nullable String exclude) {
        request.fetchSource(include, exclude);
        return this;
    }

    /**
     * Indicates whether the response should contain the updated _source.
     */
    public UpdateRequestBuilder setFetchSource(boolean fetchSource) {
        request.fetchSource(fetchSource);
        return this;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 0.
     */
    public UpdateRequestBuilder setRetryOnConflict(int retryOnConflict) {
        request.retryOnConflict(retryOnConflict);
        return this;
    }

    /**
     * only perform this update request if the document was last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public UpdateRequestBuilder setIfSeqNo(long seqNo) {
        request.setIfSeqNo(seqNo);
        return this;
    }

    /**
     * only perform this update request if the document was last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public UpdateRequestBuilder setIfPrimaryTerm(long term) {
        request.setIfPrimaryTerm(term);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(XContentBuilder source) {
        request.doc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(Map<String, Object> source) {
        request.doc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(String source, XContentType xContentType) {
        request.doc(source, xContentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public UpdateRequestBuilder setDoc(Object... source) {
        request.doc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public UpdateRequestBuilder setDoc(XContentType xContentType, Object... source) {
        request.doc(xContentType, source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(XContentBuilder source) {
        request.upsert(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists. The doc
     * includes field and value pairs.
     */
    public UpdateRequestBuilder setUpsert(Object... source) {
        request.upsert(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists. The doc
     * includes field and value pairs.
     */
    public UpdateRequestBuilder setUpsert(XContentType xContentType, Object... source) {
        request.upsert(xContentType, source);
        return this;
    }

    /**
     * Sets whether the specified doc parameter should be used as upsert document.
     */
    public UpdateRequestBuilder setDocAsUpsert(boolean shouldUpsertDoc) {
        request.docAsUpsert(shouldUpsertDoc);
        return this;
    }

    /**
     * Sets whether to perform extra effort to detect noop updates via docAsUpsert.
     * Defaults to true.
     */
    public UpdateRequestBuilder setDetectNoop(boolean detectNoop) {
        request.detectNoop(detectNoop);
        return this;
    }

    /**
     * Sets whether the script should be run in the case of an insert
     */
    public UpdateRequestBuilder setScriptedUpsert(boolean scriptedUpsert) {
        request.scriptedUpsert(scriptedUpsert);
        return this;
    }

}
