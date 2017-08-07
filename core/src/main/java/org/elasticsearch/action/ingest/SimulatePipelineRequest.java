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

package org.elasticsearch.action.ingest;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.PipelineStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.ingest.IngestDocument.MetaData;

public class SimulatePipelineRequest extends ActionRequest {

    private String id;
    private boolean verbose;
    private BytesReference source;
    private XContentType xContentType;

    /**
     * Create a new request
     * @deprecated use {@link #SimulatePipelineRequest(BytesReference, XContentType)} that does not attempt content autodetection
     */
    @Deprecated
    public SimulatePipelineRequest(BytesReference source) {
        this(source, XContentFactory.xContentType(source));
    }

    /**
     * Creates a new request with the given source and its content type
     */
    public SimulatePipelineRequest(BytesReference source, XContentType xContentType) {
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    SimulatePipelineRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public BytesReference getSource() {
        return source;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readOptionalString();
        verbose = in.readBoolean();
        source = in.readBytesReference();
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            xContentType = XContentType.readFrom(in);
        } else {
            xContentType = XContentFactory.xContentType(source);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBoolean(verbose);
        out.writeBytesReference(source);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            xContentType.writeTo(out);
        }
    }

    public static final class Fields {
        static final String PIPELINE = "pipeline";
        static final String DOCS = "docs";
        static final String SOURCE = "_source";
    }

    static class Parsed {
        private final List<IngestDocument> documents;
        private final Pipeline pipeline;
        private final boolean verbose;

        Parsed(Pipeline pipeline, List<IngestDocument> documents, boolean verbose) {
            this.pipeline = pipeline;
            this.documents = Collections.unmodifiableList(documents);
            this.verbose = verbose;
        }

        public Pipeline getPipeline() {
            return pipeline;
        }

        public List<IngestDocument> getDocuments() {
            return documents;
        }

        public boolean isVerbose() {
            return verbose;
        }
    }

    private static final Pipeline.Factory PIPELINE_FACTORY = new Pipeline.Factory();
    static final String SIMULATED_PIPELINE_ID = "_simulate_pipeline";

    static Parsed parseWithPipelineId(String pipelineId, Map<String, Object> config, boolean verbose, PipelineStore pipelineStore) {
        if (pipelineId == null) {
            throw new IllegalArgumentException("param [pipeline] is null");
        }
        Pipeline pipeline = pipelineStore.get(pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline [" + pipelineId + "] does not exist");
        }
        List<IngestDocument> ingestDocumentList = parseDocs(config);
        return new Parsed(pipeline, ingestDocumentList, verbose);
    }

    static Parsed parse(Map<String, Object> config, boolean verbose, PipelineStore pipelineStore) throws Exception {
        Map<String, Object> pipelineConfig = ConfigurationUtils.readMap(null, null, config, Fields.PIPELINE);
        Pipeline pipeline = PIPELINE_FACTORY.create(SIMULATED_PIPELINE_ID, pipelineConfig, pipelineStore.getProcessorFactories());
        List<IngestDocument> ingestDocumentList = parseDocs(config);
        return new Parsed(pipeline, ingestDocumentList, verbose);
    }

    private static List<IngestDocument> parseDocs(Map<String, Object> config) {
        List<Map<String, Object>> docs =
            ConfigurationUtils.readList(null, null, config, Fields.DOCS);
        List<IngestDocument> ingestDocumentList = new ArrayList<>();
        for (Map<String, Object> dataMap : docs) {
            Map<String, Object> document = ConfigurationUtils.readMap(null, null,
                dataMap, Fields.SOURCE);
            String index = ConfigurationUtils.readStringOrIntProperty(null, null,
                dataMap, MetaData.INDEX.getFieldName(), "_index");
            String type = ConfigurationUtils.readStringOrIntProperty(null, null,
                dataMap, MetaData.TYPE.getFieldName(), "_type");
            String id = ConfigurationUtils.readStringOrIntProperty(null, null,
                dataMap, MetaData.ID.getFieldName(), "_id");
            String routing = ConfigurationUtils.readOptionalStringOrIntProperty(null, null,
                dataMap, MetaData.ROUTING.getFieldName());
            String parent = ConfigurationUtils.readOptionalStringOrIntProperty(null, null,
                dataMap, MetaData.PARENT.getFieldName());
            IngestDocument ingestDocument =
                new IngestDocument(index, type, id, routing, parent, document);
            ingestDocumentList.add(ingestDocument);
        }
        return ingestDocumentList;
    }
}
