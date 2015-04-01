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

package org.elasticsearch.action.indexedscripts.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import java.util.Map;

/**
 * An index document action request builder.
 */
public class PutIndexedScriptRequestBuilder extends ActionRequestBuilder<PutIndexedScriptRequest, PutIndexedScriptResponse, PutIndexedScriptRequestBuilder, Client> {

    public PutIndexedScriptRequestBuilder(Client client) {
        super(client, new PutIndexedScriptRequest());
    }

    /**
     * Sets the type to index the document to.
     */
    public PutIndexedScriptRequestBuilder setScriptLang(String scriptLang) {
        request.scriptLang(scriptLang);
        return this;
    }

    /**
     * Sets the id to index the document under. Optional, and if not set, one will be automatically
     * generated.
     */
    public PutIndexedScriptRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Sets the source.
     */
    public PutIndexedScriptRequestBuilder setSource(BytesReference source) {
        request.source(source);
        return this;
    }

    /**
     * Index the Map as a JSON.
     *
     * @param source The map to index
     */
    public PutIndexedScriptRequestBuilder setSource(Map<String, Object> source) {
        request.source(source);
        return this;
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public PutIndexedScriptRequestBuilder setSource(Map<String, Object> source, XContentType contentType) {
        request.source(source, contentType);
        return this;
    }

    /**
     * Sets the document source to index.
     * <p/>
     * <p>Note, its preferable to either set it using {@link #setSource(org.elasticsearch.common.xcontent.XContentBuilder)}
     * or using the {@link #setSource(byte[])}.
     */
    public PutIndexedScriptRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the content source to index.
     */
    public PutIndexedScriptRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.source(sourceBuilder);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public PutIndexedScriptRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     */
    public PutIndexedScriptRequestBuilder setSource(byte[] source, int offset, int length) {
        request.source(source, offset, length);
        return this;
    }

    /**
     * Constructs a simple document with a field name and value pairs.
     * <b>Note: the number of objects passed to this method must be an even number.</b> 
     */
    public PutIndexedScriptRequestBuilder setSource(Object... source) {
        request.source(source);
        return this;
    }

    /**
     * The content type that will be used to generate a document from user provided objects (like Map).
     */
    public PutIndexedScriptRequestBuilder setContentType(XContentType contentType) {
        request.contentType(contentType);
        return this;
    }

    /**
     * Sets the type of operation to perform.
     */
    public PutIndexedScriptRequestBuilder setOpType(IndexRequest.OpType opType) {
        request.opType(opType);
        return this;
    }

    /**
     * Sets a string representation of the {@link #setOpType(org.elasticsearch.action.index.IndexRequest.OpType)}. Can
     * be either "index" or "create".
     */
    public PutIndexedScriptRequestBuilder setOpType(String opType) {
        request.opType(IndexRequest.OpType.fromString(opType));
        return this;
    }

    /**
     * Set to <tt>true</tt> to force this index to use {@link org.elasticsearch.action.index.IndexRequest.OpType#CREATE}.
     */
    public PutIndexedScriptRequestBuilder setCreate(boolean create) {
        request.create(create);
        return this;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public PutIndexedScriptRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public PutIndexedScriptRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }


    @Override
    protected void doExecute(final ActionListener<PutIndexedScriptResponse> listener) {
        client.putIndexedScript(request, listener);
        /*
        try {
            scriptService.putScriptToIndex(client, request.safeSource(), request.id(), request.scriptLang(), null, request.opType().toString(), new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    listener.onResponse(new PutIndexedScriptResponse(indexResponse.getType(),indexResponse.getId(),indexResponse.getVersion(),indexResponse.isCreated()));
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } catch (IOException ioe) {
            listener.onFailure(ioe);
        }
         */
    }
}
