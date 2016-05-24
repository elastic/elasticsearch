/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.update;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;

import java.util.Map;

public class UpdateRequestBuilder extends InstanceShardOperationRequestBuilder<UpdateRequest, UpdateResponse, UpdateRequestBuilder>
        implements WriteRequestBuilder<UpdateRequestBuilder> {

    public UpdateRequestBuilder(ElasticsearchClient client, UpdateAction action) {
        super(client, action, new UpdateRequest());
    }

    public UpdateRequestBuilder(ElasticsearchClient client, UpdateAction action, String index, String type, String id) {
        super(client, action, new UpdateRequest(index, type, id));
    }

    /**
     * Sets the type of the indexed document.
     */
    public UpdateRequestBuilder setType(String type) {
        request.type(type);
        return this;
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

    public UpdateRequestBuilder setParent(String parent) {
        request.parent(parent);
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
     * Explicitly specify the fields that will be returned. By default, nothing is returned.
     */
    public UpdateRequestBuilder setFields(String... fields) {
        request.fields(fields);
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
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public UpdateRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public UpdateRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public UpdateRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(IndexRequest indexRequest) {
        request.doc(indexRequest);
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
    public UpdateRequestBuilder setDoc(Map source) {
        request.doc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(Map source, XContentType contentType) {
        request.doc(source, contentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(String source) {
        request.doc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(byte[] source) {
        request.doc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(byte[] source, int offset, int length) {
        request.doc(source, offset, length);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(String field, Object value) {
        request.doc(field, value);
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
     * Sets the index request to be used if the document does not exists. Otherwise, a {@link org.elasticsearch.index.engine.DocumentMissingException}
     * is thrown.
     */
    public UpdateRequestBuilder setUpsert(IndexRequest indexRequest) {
        request.upsert(indexRequest);
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
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(Map source) {
        request.upsert(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(Map source, XContentType contentType) {
        request.upsert(source, contentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(String source) {
        request.upsert(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(byte[] source) {
        request.upsert(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsert(byte[] source, int offset, int length) {
        request.upsert(source, offset, length);
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

    public UpdateRequestBuilder setSource(XContentBuilder source) throws Exception {
        request.source(source);
        return this;
    }

    public UpdateRequestBuilder setSource(byte[] source) throws Exception {
        request.source(source);
        return this;
    }

    public UpdateRequestBuilder setSource(byte[] source, int offset, int length) throws Exception {
        request.source(source, offset, length);
        return this;
    }

    public UpdateRequestBuilder setSource(BytesReference source) throws Exception {
        request.source(source);
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
     * Defautls to true.
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

    /**
     * Set the new ttl of the document as a long. Note that if detectNoop is true (the default)
     * and the source of the document isn't changed then the ttl update won't take
     * effect.
     */
    public UpdateRequestBuilder setTtl(Long ttl) {
        request.doc().ttl(ttl);
        return this;
    }

    /**
     * Set the new ttl of the document as a time value expression. Note that if detectNoop is true (the default)
     * and the source of the document isn't changed then the ttl update won't take
     * effect.
     */
    public UpdateRequestBuilder setTtl(String ttl) {
        request.doc().ttl(ttl);
        return this;
    }

    /**
     * Set the new ttl of the document as a {@link TimeValue} instance. Note that if detectNoop is true (the default)
     * and the source of the document isn't changed then the ttl update won't take
     * effect.
     */
    public UpdateRequestBuilder setTtl(TimeValue ttl) {
        request.doc().ttl(ttl);
        return this;
    }
}
