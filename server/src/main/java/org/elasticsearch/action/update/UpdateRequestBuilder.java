/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.exception.ElasticsearchGenerationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class UpdateRequestBuilder extends InstanceShardOperationRequestBuilder<UpdateRequest, UpdateResponse, UpdateRequestBuilder>
    implements
        WriteRequestBuilder<UpdateRequestBuilder> {

    private String id;
    private String routing;
    private Script script;

    private String fetchSourceInclude;
    private String fetchSourceExclude;
    private String[] fetchSourceIncludeArray;
    private String[] fetchSourceExcludeArray;
    private Boolean fetchSource;

    private Integer retryOnConflict;
    private Long version;
    private VersionType versionType;
    private Long ifSeqNo;
    private Long ifPrimaryTerm;
    private ActiveShardCount waitForActiveShards;

    private IndexRequest doc;
    private BytesReference docSourceBytesReference;
    private XContentType docSourceXContentType;

    private IndexRequest upsert;
    private BytesReference upsertSourceBytesReference;
    private XContentType upsertSourceXContentType;

    private Boolean docAsUpsert;
    private Boolean detectNoop;
    private Boolean scriptedUpsert;
    private Boolean requireAlias;
    private WriteRequest.RefreshPolicy refreshPolicy;

    public UpdateRequestBuilder(ElasticsearchClient client) {
        this(client, null, null);
    }

    @SuppressWarnings("this-escape")
    public UpdateRequestBuilder(ElasticsearchClient client, String index, String id) {
        super(client, TransportUpdateAction.TYPE);
        setIndex(index);
        setId(id);
    }

    /**
     * Sets the id of the indexed document.
     */
    public UpdateRequestBuilder setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public UpdateRequestBuilder setRouting(String routing) {
        this.routing = routing;
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
        this.script = script;
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
        this.fetchSourceInclude = include;
        this.fetchSourceExclude = exclude;
        return this;
    }

    /**
     * Indicate that _source should be returned, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes
     *            An optional list of include (optionally wildcarded) pattern to
     *            filter the returned _source
     * @param excludes
     *            An optional list of exclude (optionally wildcarded) pattern to
     *            filter the returned _source
     */
    public UpdateRequestBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        this.fetchSourceIncludeArray = includes;
        this.fetchSourceExcludeArray = excludes;
        return this;
    }

    /**
     * Indicates whether the response should contain the updated _source.
     */
    public UpdateRequestBuilder setFetchSource(boolean fetchSource) {
        this.fetchSource = fetchSource;
        return this;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 0.
     */
    public UpdateRequestBuilder setRetryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
        return this;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public UpdateRequestBuilder setVersion(long version) {
        this.version = version;
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public UpdateRequestBuilder setVersionType(VersionType versionType) {
        this.versionType = versionType;
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
        this.ifSeqNo = seqNo;
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
        this.ifPrimaryTerm = term;
        return this;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public UpdateRequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public UpdateRequestBuilder setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(IndexRequest indexRequest) {
        this.doc = indexRequest;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(XContentBuilder source) {
        this.docSourceBytesReference = BytesReference.bytes(source);
        this.docSourceXContentType = source.contentType();
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(Map<String, Object> source) {
        return setDoc(source, Requests.INDEX_CONTENT_TYPE);
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(Map<String, Object> source, XContentType contentType) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return setDoc(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(String source, XContentType xContentType) {
        this.docSourceBytesReference = new BytesArray(source);
        this.docSourceXContentType = xContentType;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(byte[] source, XContentType xContentType) {
        return setDoc(source, 0, source.length, xContentType);
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(byte[] source, int offset, int length, XContentType xContentType) {
        this.docSourceBytesReference = new BytesArray(source, offset, length);
        this.docSourceXContentType = xContentType;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public UpdateRequestBuilder setDoc(Object... source) {
        return setDoc(Requests.INDEX_CONTENT_TYPE, source);
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public UpdateRequestBuilder setDoc(XContentType xContentType, Object... source) {
        return setDoc(IndexRequest.getXContentBuilder(xContentType, source));
    }

    /**
     * Sets the index request to be used if the document does not exists. Otherwise, a
     * {@link org.elasticsearch.index.engine.DocumentMissingException} is thrown.
     */
    public UpdateRequestBuilder setUpsert(IndexRequest indexRequest) {
        this.upsert = indexRequest;
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(XContentBuilder source) {
        this.upsertSourceBytesReference = BytesReference.bytes(source);
        this.upsertSourceXContentType = source.contentType();
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(Map<String, Object> source) {
        return setUpsert(source, Requests.INDEX_CONTENT_TYPE);
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(Map<String, Object> source, XContentType contentType) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return setUpsert(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(String source, XContentType xContentType) {
        this.upsertSourceBytesReference = new BytesArray(source);
        this.upsertSourceXContentType = xContentType;
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(byte[] source, XContentType xContentType) {
        return setUpsert(source, 0, source.length, xContentType);
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(byte[] source, int offset, int length, XContentType xContentType) {
        this.upsertSourceBytesReference = new BytesArray(source, offset, length);
        this.upsertSourceXContentType = xContentType;
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists. The doc
     * includes field and value pairs.
     */
    public UpdateRequestBuilder setUpsert(Object... source) {
        return setUpsert(Requests.INDEX_CONTENT_TYPE, source);
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists. The doc
     * includes field and value pairs.
     */
    public UpdateRequestBuilder setUpsert(XContentType xContentType, Object... source) {
        return setUpsert(IndexRequest.getXContentBuilder(xContentType, source));
    }

    /**
     * Sets whether the specified doc parameter should be used as upsert document.
     */
    public UpdateRequestBuilder setDocAsUpsert(boolean shouldUpsertDoc) {
        this.docAsUpsert = shouldUpsertDoc;
        return this;
    }

    /**
     * Sets whether to perform extra effort to detect noop updates via docAsUpsert.
     * Defaults to true.
     */
    public UpdateRequestBuilder setDetectNoop(boolean detectNoop) {
        this.detectNoop = detectNoop;
        return this;
    }

    /**
     * Sets whether the script should be run in the case of an insert
     */
    public UpdateRequestBuilder setScriptedUpsert(boolean scriptedUpsert) {
        this.scriptedUpsert = scriptedUpsert;
        return this;
    }

    /**
     * Sets the require_alias flag
     */
    public UpdateRequestBuilder setRequireAlias(boolean requireAlias) {
        this.requireAlias = requireAlias;
        return this;
    }

    @Override
    public UpdateRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public UpdateRequestBuilder setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicy = WriteRequest.RefreshPolicy.parse(refreshPolicy);
        return this;
    }

    @Override
    public UpdateRequest request() {
        validate();
        UpdateRequest request = new UpdateRequest();
        super.apply(request);
        if (id != null) {
            request.id(id);
        }
        if (routing != null) {
            request.routing(routing);
        }
        if (script != null) {
            request.script(script);
        }
        if (fetchSourceInclude != null || fetchSourceExclude != null) {
            request.fetchSource(fetchSourceInclude, fetchSourceExclude);
        }
        if (fetchSourceIncludeArray != null || fetchSourceExcludeArray != null) {
            request.fetchSource(fetchSourceIncludeArray, fetchSourceExcludeArray);
        }
        if (fetchSource != null) {
            request.fetchSource(fetchSource);
        }
        if (retryOnConflict != null) {
            request.retryOnConflict(retryOnConflict);
        }
        if (version != null) {
            request.version(version);
        }
        if (versionType != null) {
            request.versionType(versionType);
        }
        if (ifSeqNo != null) {
            request.setIfSeqNo(ifSeqNo);
        }
        if (ifPrimaryTerm != null) {
            request.setIfPrimaryTerm(ifPrimaryTerm);
        }
        if (waitForActiveShards != null) {
            request.waitForActiveShards(waitForActiveShards);
        }
        if (doc != null) {
            request.doc(doc);
        }
        if (docSourceBytesReference != null && docSourceXContentType != null) {
            request.doc(docSourceBytesReference, docSourceXContentType);
        }
        if (upsert != null) {
            request.upsert(upsert);
        }
        if (upsertSourceBytesReference != null && upsertSourceXContentType != null) {
            request.upsert(upsertSourceBytesReference, upsertSourceXContentType);
        }
        if (docAsUpsert != null) {
            request.docAsUpsert(docAsUpsert);
        }
        if (detectNoop != null) {
            request.detectNoop(detectNoop);
        }
        if (scriptedUpsert != null) {
            request.scriptedUpsert(scriptedUpsert);
        }
        if (requireAlias != null) {
            request.setRequireAlias(requireAlias);
        }
        if (refreshPolicy != null) {
            request.setRefreshPolicy(refreshPolicy);
        }
        return request;
    }

    protected void validate() throws IllegalStateException {
        boolean fetchIncludeExcludeNotNull = fetchSourceInclude != null || fetchSourceExclude != null;
        boolean fetchIncludeExcludeArrayNotNull = fetchSourceIncludeArray != null || fetchSourceExcludeArray != null;
        boolean fetchSourceNotNull = fetchSource != null;
        if ((fetchIncludeExcludeNotNull && fetchIncludeExcludeArrayNotNull)
            || (fetchIncludeExcludeNotNull && fetchSourceNotNull)
            || (fetchIncludeExcludeArrayNotNull && fetchSourceNotNull)) {
            throw new IllegalStateException("Only one fetchSource() method may be called");
        }
        int docSourceFieldsSet = countDocSourceFieldsSet();
        if (docSourceFieldsSet > 1) {
            throw new IllegalStateException("Only one setDoc() method may be called, but " + docSourceFieldsSet + " have been");
        }
        int upsertSourceFieldsSet = countUpsertSourceFieldsSet();
        if (upsertSourceFieldsSet > 1) {
            throw new IllegalStateException("Only one setUpsert() method may be called, but " + upsertSourceFieldsSet + " have been");
        }
    }

    private int countDocSourceFieldsSet() {
        return countNonNullObjects(doc, docSourceBytesReference);
    }

    private int countUpsertSourceFieldsSet() {
        return countNonNullObjects(upsert, upsertSourceBytesReference);
    }

    private int countNonNullObjects(Object... objects) {
        int sum = 0;
        for (Object object : objects) {
            if (object != null) {
                sum++;
            }
        }
        return sum;
    }
}
