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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Create snapshot request
 * <p/>
 * The only mandatory parameter is repository name. The repository name has to satisfy the following requirements
 * <ul>
 * <li>be a non-empty string</li>
 * <li>must not contain whitespace (tabs or spaces)</li>
 * <li>must not contain comma (',')</li>
 * <li>must not contain hash sign ('#')</li>
 * <li>must not start with underscore ('-')</li>
 * <li>must be lowercase</li>
 * <li>must not contain invalid file name characters {@link org.elasticsearch.common.Strings#INVALID_FILENAME_CHARS} </li>
 * </ul>
 */
public class CreateSnapshotRequest extends MasterNodeRequest<CreateSnapshotRequest> implements IndicesRequest.Replaceable {

    private String snapshot;

    private String repository;

    private String[] indices = EMPTY_ARRAY;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    private boolean partial = false;

    private Settings settings = EMPTY_SETTINGS;

    private boolean includeGlobalState = true;

    private boolean waitForCompletion;

    public CreateSnapshotRequest() {
    }

    /**
     * Constructs a new put repository request with the provided snapshot and repository names
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public CreateSnapshotRequest(String repository, String snapshot) {
        this.snapshot = snapshot;
        this.repository = repository;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (snapshot == null) {
            validationException = addValidationError("snapshot is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices is null", validationException);
        } else {
            for (String index : indices) {
                if (index == null) {
                    validationException = addValidationError("index is null", validationException);
                    break;
                }
            }
        }
        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is null", validationException);
        }
        if (settings == null) {
            validationException = addValidationError("settings is null", validationException);
        }
        return validationException;
    }

    /**
     * Sets the snapshot name
     *
     * @param snapshot snapshot name
     */
    public CreateSnapshotRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    /**
     * The snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Sets repository name
     *
     * @param repository name
     * @return this request
     */
    public CreateSnapshotRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return this.repository;
    }

    /**
     * Sets a list of indices that should be included into the snapshot
     * <p/>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @param indices
     * @return this request
     */
    @Override
    public CreateSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets a list of indices that should be included into the snapshot
     * <p/>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @param indices
     * @return this request
     */
    public CreateSnapshotRequest indices(List<String> indices) {
        this.indices = indices.toArray(new String[indices.size()]);
        return this;
    }

    /**
     * Returns a list of indices that should be included into the snapshot
     *
     * @return list of indices
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices options
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CreateSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }


    /**
     * Returns true if indices with unavailable shards should be be partially snapshotted.
     *
     * @return the desired behaviour regarding indices options
     */
    public boolean partial() {
        return partial;
    }

    /**
     * Set to true to allow indices with unavailable shards to be partially snapshotted.
     *
     * @param partial true if indices with unavailable shards should be be partially snapshotted.
     * @return this request
     */
    public CreateSnapshotRequest partial(boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * If set to true the operation should wait for the snapshot completion before returning.
     *
     * By default, the operation will return as soon as snapshot is initialized. It can be changed by setting this
     * flag to true.
     *
     * @param waitForCompletion true if operation should wait for the snapshot completion
     * @return this request
     */
    public CreateSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns true if the request should wait for the snapshot completion before returning
     *
     * @return true if the request should wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p/>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p/>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets repository-specific snapshot settings in JSON, YAML or properties format
     * <p/>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(String source) {
        this.settings = Settings.settingsBuilder().loadFromSource(source).build();
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p/>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @return this request
     */
    public CreateSnapshotRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Returns repository-specific snapshot settings
     *
     * @return repository-specific snapshot settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Set to true if global state should be stored as part of the snapshot
     *
     * @param includeGlobalState true if global state should be stored
     * @return this request
     */
    public CreateSnapshotRequest includeGlobalState(boolean includeGlobalState) {
        this.includeGlobalState = includeGlobalState;
        return this;
    }

    /**
     * Returns true if global state should be stored as part of the snapshot
     *
     * @return true if global state should be stored as part of the snapshot
     */
    public boolean includeGlobalState() {
        return includeGlobalState;
    }

    /**
     * Parses snapshot definition.
     *
     * @param source snapshot definition
     * @return this request
     */
    public CreateSnapshotRequest source(XContentBuilder source) {
        return source(source.bytes());
    }

    /**
     * Parses snapshot definition.
     *
     * @param source snapshot definition
     * @return this request
     */
    public CreateSnapshotRequest source(Map source) {
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) source).entrySet()) {
            String name = entry.getKey();
            if (name.equals("indices")) {
                if (entry.getValue() instanceof String) {
                    indices(Strings.splitStringByCommaToArray((String) entry.getValue()));
                } else if (entry.getValue() instanceof ArrayList) {
                    indices((ArrayList<String>) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed indices section, should be an array of strings");
                }
            } else if (name.equals("partial")) {
                partial(nodeBooleanValue(entry.getValue()));
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("malformed settings section, should indices an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("include_global_state")) {
                includeGlobalState = nodeBooleanValue(entry.getValue());
            }
        }
        indicesOptions(IndicesOptions.fromMap((Map<String, Object>) source, IndicesOptions.lenientExpandOpen()));
        return this;
    }

    /**
     * Parses snapshot definition. JSON, YAML and properties formats are supported
     *
     * @param source snapshot definition
     * @return this request
     */
    public CreateSnapshotRequest source(String source) {
        if (hasLength(source)) {
            try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
                return source(parser.mapOrdered());
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to parse repository source [" + source + "]", e);
            }
        }
        return this;
    }

    /**
     * Parses snapshot definition. JSON, YAML and properties formats are supported
     *
     * @param source snapshot definition
     * @return this request
     */
    public CreateSnapshotRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * Parses snapshot definition. JSON, YAML and properties formats are supported
     *
     * @param source snapshot definition
     * @param offset offset
     * @param length length
     * @return this request
     */
    public CreateSnapshotRequest source(byte[] source, int offset, int length) {
        if (length > 0) {
            try (XContentParser parser = XContentFactory.xContent(source, offset, length).createParser(source, offset, length)) {
                return source(parser.mapOrdered());
            } catch (IOException e) {
                throw new IllegalArgumentException("failed to parse repository source", e);
            }
        }
        return this;
    }

    /**
     * Parses snapshot definition. JSON, YAML and properties formats are supported
     *
     * @param source snapshot definition
     * @return this request
     */
    public CreateSnapshotRequest source(BytesReference source) {
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            return source(parser.mapOrdered());
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse snapshot source", e);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        snapshot = in.readString();
        repository = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        settings = readSettingsFromStream(in);
        includeGlobalState = in.readBoolean();
        waitForCompletion = in.readBoolean();
        partial = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        writeSettingsToStream(settings, out);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(partial);
    }
}
