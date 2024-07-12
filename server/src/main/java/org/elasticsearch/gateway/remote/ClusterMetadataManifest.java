/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.gateway.remote;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.common.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Manifest file which contains the details of the uploaded entity metadata
 *
 * @opensearch.internal
 */
public class ClusterMetadataManifest implements Writeable, ToXContentFragment {

    public static final int CODEC_V0 = 0; // Older codec version, where we haven't introduced codec versions for manifest.
    public static final int CODEC_V1 = 1; // In Codec V1 we have introduced global-metadata and codec version in Manifest file.
    public static final int CODEC_V2 = 2; // In Codec V2, there are separate metadata files rather than a single global metadata file,
    // also we introduce index routing-metadata, diff and other attributes as part of manifest
    // required for state publication

    private static final ParseField CLUSTER_TERM_FIELD = new ParseField("cluster_term");
    private static final ParseField STATE_VERSION_FIELD = new ParseField("state_version");
    private static final ParseField CLUSTER_UUID_FIELD = new ParseField("cluster_uuid");
    private static final ParseField STATE_UUID_FIELD = new ParseField("state_uuid");
    private static final ParseField OPENSEARCH_VERSION_FIELD = new ParseField("opensearch_version");
    private static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    private static final ParseField COMMITTED_FIELD = new ParseField("committed");
    private static final ParseField CODEC_VERSION_FIELD = new ParseField("codec_version");
    private static final ParseField GLOBAL_METADATA_FIELD = new ParseField("global_metadata");
    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField PREVIOUS_CLUSTER_UUID = new ParseField("previous_cluster_uuid");
    private static final ParseField CLUSTER_UUID_COMMITTED = new ParseField("cluster_uuid_committed");
    private static final ParseField UPLOADED_COORDINATOR_METADATA = new ParseField("uploaded_coordinator_metadata");
    private static final ParseField UPLOADED_SETTINGS_METADATA = new ParseField("uploaded_settings_metadata");
    private static final ParseField UPLOADED_TEMPLATES_METADATA = new ParseField("uploaded_templates_metadata");
    private static final ParseField UPLOADED_CUSTOM_METADATA = new ParseField("uploaded_custom_metadata");
    private static final ParseField ROUTING_TABLE_VERSION_FIELD = new ParseField("routing_table_version");
    private static final ParseField INDICES_ROUTING_FIELD = new ParseField("indices_routing");
    private static final ParseField METADATA_VERSION = new ParseField("metadata_version");
    private static final ParseField UPLOADED_TRANSIENT_SETTINGS_METADATA = new ParseField("uploaded_transient_settings_metadata");
    private static final ParseField UPLOADED_DISCOVERY_NODES_METADATA = new ParseField("uploaded_discovery_nodes_metadata");
    private static final ParseField UPLOADED_CLUSTER_BLOCKS_METADATA = new ParseField("uploaded_cluster_blocks_metadata");
    private static final ParseField UPLOADED_HASHES_OF_CONSISTENT_SETTINGS_METADATA = new ParseField(
        "uploaded_hashes_of_consistent_settings_metadata"
    );
    private static final ParseField UPLOADED_CLUSTER_STATE_CUSTOM_METADATA = new ParseField("uploaded_cluster_state_custom_metadata");
    private static final ParseField DIFF_MANIFEST = new ParseField("diff_manifest");

    private static ClusterMetadataManifest.Builder manifestV0Builder(Object[] fields) {
        return ClusterMetadataManifest.builder()
            .clusterTerm(term(fields))
            .stateVersion(version(fields))
            .clusterUUID(clusterUUID(fields))
            .stateUUID(stateUUID(fields))
            .opensearchVersion(opensearchVersion(fields))
            .nodeId(nodeId(fields))
            .committed(committed(fields))
            .codecVersion(CODEC_V0)
            .indices(indices(fields))
            .previousClusterUUID(previousClusterUUID(fields))
            .clusterUUIDCommitted(clusterUUIDCommitted(fields));
    }

    private static ClusterMetadataManifest.Builder manifestV1Builder(Object[] fields) {
        return manifestV0Builder(fields).codecVersion(codecVersion(fields)).globalMetadataFileName(globalMetadataFileName(fields));
    }

    private static ClusterMetadataManifest.Builder manifestV2Builder(Object[] fields) {
        return manifestV0Builder(fields).codecVersion(codecVersion(fields))
            .coordinationMetadata(coordinationMetadata(fields))
            .settingMetadata(settingsMetadata(fields))
            .templatesMetadata(templatesMetadata(fields))
            .customMetadataMap(customMetadata(fields))
            .routingTableVersion(routingTableVersion(fields))
            .indicesRouting(indicesRouting(fields))
            .discoveryNodesMetadata(discoveryNodesMetadata(fields))
            .clusterBlocksMetadata(clusterBlocksMetadata(fields))
            .diffManifest(diffManifest(fields))
            .metadataVersion(metadataVersion(fields))
            .transientSettingsMetadata(transientSettingsMetadata(fields))
            .hashesOfConsistentSettings(hashesOfConsistentSettings(fields))
            .clusterStateCustomMetadataMap(clusterStateCustomMetadata(fields));
    }

    private static long term(Object[] fields) {
        return (long) fields[0];
    }

    private static long version(Object[] fields) {
        return (long) fields[1];
    }

    private static String clusterUUID(Object[] fields) {
        return (String) fields[2];
    }

    private static String stateUUID(Object[] fields) {
        return (String) fields[3];
    }

    private static Version opensearchVersion(Object[] fields) {
        return Version.fromId((int) fields[4]);
    }

    private static String nodeId(Object[] fields) {
        return (String) fields[5];
    }

    private static boolean committed(Object[] fields) {
        return (boolean) fields[6];
    }

    private static List<UploadedIndexMetadata> indices(Object[] fields) {
        return (List<UploadedIndexMetadata>) fields[7];
    }

    private static String previousClusterUUID(Object[] fields) {
        return (String) fields[8];
    }

    private static boolean clusterUUIDCommitted(Object[] fields) {
        return (boolean) fields[9];
    }

    private static int codecVersion(Object[] fields) {
        return (int) fields[10];
    }

    private static String globalMetadataFileName(Object[] fields) {
        return (String) fields[11];
    }

    private static UploadedMetadataAttribute coordinationMetadata(Object[] fields) {
        return (UploadedMetadataAttribute) fields[11];
    }

    private static UploadedMetadataAttribute settingsMetadata(Object[] fields) {
        return (UploadedMetadataAttribute) fields[12];
    }

    private static UploadedMetadataAttribute templatesMetadata(Object[] fields) {
        return (UploadedMetadataAttribute) fields[13];
    }

    private static Map<String, UploadedMetadataAttribute> customMetadata(Object[] fields) {
        List<UploadedMetadataAttribute> customs = (List<UploadedMetadataAttribute>) fields[14];
        return customs.stream().collect(Collectors.toMap(UploadedMetadataAttribute::getAttributeName, Function.identity()));
    }

    private static long routingTableVersion(Object[] fields) {
        return (long) fields[15];
    }

    private static List<UploadedIndexMetadata> indicesRouting(Object[] fields) {
        return (List<UploadedIndexMetadata>) fields[16];
    }

    private static UploadedMetadataAttribute discoveryNodesMetadata(Object[] fields) {
        return (UploadedMetadataAttribute) fields[17];
    }

    private static UploadedMetadataAttribute clusterBlocksMetadata(Object[] fields) {
        return (UploadedMetadataAttribute) fields[18];
    }

    private static long metadataVersion(Object[] fields) {
        return (long) fields[19];
    }

    private static UploadedMetadataAttribute transientSettingsMetadata(Object[] fields) {
        return (UploadedMetadataAttribute) fields[20];
    }

    private static UploadedMetadataAttribute hashesOfConsistentSettings(Object[] fields) {
        return (UploadedMetadataAttribute) fields[21];
    }

    private static Map<String, UploadedMetadataAttribute> clusterStateCustomMetadata(Object[] fields) {
        List<UploadedMetadataAttribute> customs = (List<UploadedMetadataAttribute>) fields[22];
        return customs.stream().collect(Collectors.toMap(UploadedMetadataAttribute::getAttributeName, Function.identity()));
    }

    private static ClusterStateDiffManifest diffManifest(Object[] fields) {
        return (ClusterStateDiffManifest) fields[23];
    }

    private static final ConstructingObjectParser<ClusterMetadataManifest, Void> PARSER_V0 = new ConstructingObjectParser<>(
        "cluster_metadata_manifest",
        fields -> manifestV0Builder(fields).build()
    );

    private static final ConstructingObjectParser<ClusterMetadataManifest, Void> PARSER_V1 = new ConstructingObjectParser<>(
        "cluster_metadata_manifest",
        fields -> manifestV1Builder(fields).build()
    );

    private static final ConstructingObjectParser<ClusterMetadataManifest, Void> PARSER_V2 = new ConstructingObjectParser<>(
        "cluster_metadata_manifest",
        fields -> manifestV2Builder(fields).build()
    );

    private static final ConstructingObjectParser<ClusterMetadataManifest, Void> CURRENT_PARSER = PARSER_V2;

    static {
        declareParser(PARSER_V0, CODEC_V0);
        declareParser(PARSER_V1, CODEC_V1);
        declareParser(PARSER_V2, CODEC_V2);
    }

    private static void declareParser(ConstructingObjectParser<ClusterMetadataManifest, Void> parser, long codec_version) {
        parser.declareLong(ConstructingObjectParser.constructorArg(), CLUSTER_TERM_FIELD);
        parser.declareLong(ConstructingObjectParser.constructorArg(), STATE_VERSION_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), CLUSTER_UUID_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), STATE_UUID_FIELD);
        parser.declareInt(ConstructingObjectParser.constructorArg(), OPENSEARCH_VERSION_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_FIELD);
        parser.declareBoolean(ConstructingObjectParser.constructorArg(), COMMITTED_FIELD);
        parser.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> UploadedIndexMetadata.fromXContent(p),
            INDICES_FIELD
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), PREVIOUS_CLUSTER_UUID);
        parser.declareBoolean(ConstructingObjectParser.constructorArg(), CLUSTER_UUID_COMMITTED);

        if (codec_version == CODEC_V1) {
            parser.declareInt(ConstructingObjectParser.constructorArg(), CODEC_VERSION_FIELD);
            parser.declareString(ConstructingObjectParser.constructorArg(), GLOBAL_METADATA_FIELD);
        } else if (codec_version >= CODEC_V2) {
            parser.declareInt(ConstructingObjectParser.constructorArg(), CODEC_VERSION_FIELD);
            parser.declareNamedObject(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_COORDINATOR_METADATA
            );
            parser.declareNamedObject(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_SETTINGS_METADATA
            );
            parser.declareNamedObject(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_TEMPLATES_METADATA
            );
            parser.declareNamedObjects(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_CUSTOM_METADATA
            );
            parser.declareLong(ConstructingObjectParser.constructorArg(), ROUTING_TABLE_VERSION_FIELD);
            parser.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> UploadedIndexMetadata.fromXContent(p),
                INDICES_ROUTING_FIELD
            );
            parser.declareNamedObject(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_DISCOVERY_NODES_METADATA
            );
            parser.declareNamedObject(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_CLUSTER_BLOCKS_METADATA
            );
            parser.declareLong(ConstructingObjectParser.constructorArg(), METADATA_VERSION);
            parser.declareNamedObject(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_TRANSIENT_SETTINGS_METADATA
            );
            parser.declareNamedObject(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_HASHES_OF_CONSISTENT_SETTINGS_METADATA
            );
            parser.declareNamedObjects(
                ConstructingObjectParser.optionalConstructorArg(),
                UploadedMetadataAttribute.PARSER,
                UPLOADED_CLUSTER_STATE_CUSTOM_METADATA
            );
            parser.declareObject(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ClusterStateDiffManifest.fromXContent(p),
                DIFF_MANIFEST
            );
        }
    }

    private final int codecVersion;
    private final String globalMetadataFileName;
    private final UploadedMetadataAttribute uploadedCoordinationMetadata;
    private final UploadedMetadataAttribute uploadedSettingsMetadata;
    private final UploadedMetadataAttribute uploadedTemplatesMetadata;
    private final Map<String, UploadedMetadataAttribute> uploadedCustomMetadataMap;
    private final List<UploadedIndexMetadata> indices;
    private final long clusterTerm;
    private final long stateVersion;
    private final String clusterUUID;
    private final String stateUUID;
    private final Version opensearchVersion;
    private final String nodeId;
    private final boolean committed;
    private final String previousClusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long routingTableVersion;
    private final List<UploadedIndexMetadata> indicesRouting;
    private final long metadataVersion;
    private final UploadedMetadataAttribute uploadedTransientSettingsMetadata;
    private final UploadedMetadataAttribute uploadedDiscoveryNodesMetadata;
    private final UploadedMetadataAttribute uploadedClusterBlocksMetadata;
    private final UploadedMetadataAttribute uploadedHashesOfConsistentSettings;
    private final Map<String, UploadedMetadataAttribute> uploadedClusterStateCustomMap;
    private final ClusterStateDiffManifest diffManifest;

    public List<UploadedIndexMetadata> getIndices() {
        return indices;
    }

    public long getClusterTerm() {
        return clusterTerm;
    }

    public long getStateVersion() {
        return stateVersion;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public String getStateUUID() {
        return stateUUID;
    }

    public Version getOpensearchVersion() {
        return opensearchVersion;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isCommitted() {
        return committed;
    }

    public String getPreviousClusterUUID() {
        return previousClusterUUID;
    }

    public boolean isClusterUUIDCommitted() {
        return clusterUUIDCommitted;
    }

    public int getCodecVersion() {
        return codecVersion;
    }

    public String getGlobalMetadataFileName() {
        return globalMetadataFileName;
    }

    public UploadedMetadataAttribute getCoordinationMetadata() {
        return uploadedCoordinationMetadata;
    }

    public UploadedMetadataAttribute getSettingsMetadata() {
        return uploadedSettingsMetadata;
    }

    public UploadedMetadataAttribute getTemplatesMetadata() {
        return uploadedTemplatesMetadata;
    }

    public Map<String, UploadedMetadataAttribute> getCustomMetadataMap() {
        return uploadedCustomMetadataMap;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    public UploadedMetadataAttribute getTransientSettingsMetadata() {
        return uploadedTransientSettingsMetadata;
    }

    public UploadedMetadataAttribute getDiscoveryNodesMetadata() {
        return uploadedDiscoveryNodesMetadata;
    }

    public UploadedMetadataAttribute getClusterBlocksMetadata() {
        return uploadedClusterBlocksMetadata;
    }

    public ClusterStateDiffManifest getDiffManifest() {
        return diffManifest;
    }

    public Map<String, UploadedMetadataAttribute> getClusterStateCustomMap() {
        return uploadedClusterStateCustomMap;
    }

    public UploadedMetadataAttribute getHashesOfConsistentSettings() {
        return uploadedHashesOfConsistentSettings;
    }

    public boolean hasMetadataAttributesFiles() {
        return uploadedCoordinationMetadata != null
            || uploadedSettingsMetadata != null
            || uploadedTemplatesMetadata != null
            || !uploadedCustomMetadataMap.isEmpty();
    }

    public long getRoutingTableVersion() {
        return routingTableVersion;
    }

    public List<UploadedIndexMetadata> getIndicesRouting() {
        return indicesRouting;
    }

    public ClusterMetadataManifest(
        long clusterTerm,
        long version,
        String clusterUUID,
        String stateUUID,
        Version opensearchVersion,
        String nodeId,
        boolean committed,
        int codecVersion,
        String globalMetadataFileName,
        List<UploadedIndexMetadata> indices,
        String previousClusterUUID,
        boolean clusterUUIDCommitted,
        UploadedMetadataAttribute uploadedCoordinationMetadata,
        UploadedMetadataAttribute uploadedSettingsMetadata,
        UploadedMetadataAttribute uploadedTemplatesMetadata,
        Map<String, UploadedMetadataAttribute> uploadedCustomMetadataMap,
        long routingTableVersion,
        List<UploadedIndexMetadata> indicesRouting,
        long metadataVersion,
        UploadedMetadataAttribute discoveryNodesMetadata,
        UploadedMetadataAttribute clusterBlocksMetadata,
        UploadedMetadataAttribute uploadedTransientSettingsMetadata,
        UploadedMetadataAttribute uploadedHashesOfConsistentSettings,
        Map<String, UploadedMetadataAttribute> uploadedClusterStateCustomMap,
        ClusterStateDiffManifest diffManifest
    ) {
        this.clusterTerm = clusterTerm;
        this.stateVersion = version;
        this.clusterUUID = clusterUUID;
        this.stateUUID = stateUUID;
        this.opensearchVersion = opensearchVersion;
        this.nodeId = nodeId;
        this.committed = committed;
        this.codecVersion = codecVersion;
        this.globalMetadataFileName = globalMetadataFileName;
        this.indices = Collections.unmodifiableList(indices);
        this.previousClusterUUID = previousClusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.routingTableVersion = routingTableVersion;
        this.indicesRouting = Collections.unmodifiableList(indicesRouting);
        this.uploadedCoordinationMetadata = uploadedCoordinationMetadata;
        this.uploadedSettingsMetadata = uploadedSettingsMetadata;
        this.uploadedTemplatesMetadata = uploadedTemplatesMetadata;
        this.uploadedCustomMetadataMap = Collections.unmodifiableMap(
            uploadedCustomMetadataMap != null ? uploadedCustomMetadataMap : new HashMap<>()
        );
        this.uploadedDiscoveryNodesMetadata = discoveryNodesMetadata;
        this.uploadedClusterBlocksMetadata = clusterBlocksMetadata;
        this.diffManifest = diffManifest;
        this.metadataVersion = metadataVersion;
        this.uploadedTransientSettingsMetadata = uploadedTransientSettingsMetadata;
        this.uploadedHashesOfConsistentSettings = uploadedHashesOfConsistentSettings;
        this.uploadedClusterStateCustomMap = Collections.unmodifiableMap(
            uploadedClusterStateCustomMap != null ? uploadedClusterStateCustomMap : new HashMap<>()
        );
    }

    public ClusterMetadataManifest(StreamInput in) throws IOException {
        this.clusterTerm = in.readVLong();
        this.stateVersion = in.readVLong();
        this.clusterUUID = in.readString();
        this.stateUUID = in.readString();
        this.opensearchVersion = Version.fromId(in.readInt());
        this.nodeId = in.readString();
        this.committed = in.readBoolean();
        this.indices = Collections.unmodifiableList(in.readList(UploadedIndexMetadata::new));
        this.previousClusterUUID = in.readString();
        this.clusterUUIDCommitted = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_2_15_0)) {
            this.codecVersion = in.readInt();
            this.uploadedCoordinationMetadata = new UploadedMetadataAttribute(in);
            this.uploadedSettingsMetadata = new UploadedMetadataAttribute(in);
            this.uploadedTemplatesMetadata = new UploadedMetadataAttribute(in);
            this.uploadedCustomMetadataMap = Collections.unmodifiableMap(
                in.readMap(StreamInput::readString, UploadedMetadataAttribute::new)
            );
            this.globalMetadataFileName = null;
            this.routingTableVersion = in.readLong();
            this.indicesRouting = Collections.unmodifiableList(in.readList(UploadedIndexMetadata::new));
            this.metadataVersion = in.readLong();
            if (in.readBoolean()) {
                this.uploadedDiscoveryNodesMetadata = new UploadedMetadataAttribute(in);
            } else {
                this.uploadedDiscoveryNodesMetadata = null;
            }
            if (in.readBoolean()) {
                this.uploadedClusterBlocksMetadata = new UploadedMetadataAttribute(in);
            } else {
                this.uploadedClusterBlocksMetadata = null;
            }
            if (in.readBoolean()) {
                this.uploadedTransientSettingsMetadata = new UploadedMetadataAttribute(in);
            } else {
                this.uploadedTransientSettingsMetadata = null;
            }
            if (in.readBoolean()) {
                this.uploadedHashesOfConsistentSettings = new UploadedMetadataAttribute(in);
            } else {
                this.uploadedHashesOfConsistentSettings = null;
            }
            this.uploadedClusterStateCustomMap = Collections.unmodifiableMap(
                in.readMap(StreamInput::readString, UploadedMetadataAttribute::new)
            );
            if (in.readBoolean()) {
                this.diffManifest = new ClusterStateDiffManifest(in);
            } else {
                this.diffManifest = null;
            }
        } else {
            if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
                this.codecVersion = in.readInt();
                this.globalMetadataFileName = in.readString();
            } else {
                this.codecVersion = CODEC_V0; // Default codec
                this.globalMetadataFileName = null;
            }
            this.uploadedCoordinationMetadata = null;
            this.uploadedSettingsMetadata = null;
            this.uploadedTemplatesMetadata = null;
            this.uploadedCustomMetadataMap = null;
            this.routingTableVersion = -1;
            this.indicesRouting = null;
            this.uploadedDiscoveryNodesMetadata = null;
            this.uploadedClusterBlocksMetadata = null;
            this.diffManifest = null;
            this.metadataVersion = -1;
            this.uploadedTransientSettingsMetadata = null;
            this.uploadedHashesOfConsistentSettings = null;
            this.uploadedClusterStateCustomMap = null;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ClusterMetadataManifest manifest) {
        return new Builder(manifest);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CLUSTER_TERM_FIELD.getPreferredName(), getClusterTerm())
            .field(STATE_VERSION_FIELD.getPreferredName(), getStateVersion())
            .field(CLUSTER_UUID_FIELD.getPreferredName(), getClusterUUID())
            .field(STATE_UUID_FIELD.getPreferredName(), getStateUUID())
            .field(OPENSEARCH_VERSION_FIELD.getPreferredName(), getOpensearchVersion().id)
            .field(NODE_ID_FIELD.getPreferredName(), getNodeId())
            .field(COMMITTED_FIELD.getPreferredName(), isCommitted());
        builder.startArray(INDICES_FIELD.getPreferredName());
        {
            for (UploadedIndexMetadata uploadedIndexMetadata : indices) {
                builder.startObject();
                uploadedIndexMetadata.toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endArray();
        builder.field(PREVIOUS_CLUSTER_UUID.getPreferredName(), getPreviousClusterUUID());
        builder.field(CLUSTER_UUID_COMMITTED.getPreferredName(), isClusterUUIDCommitted());
        if (onOrAfterCodecVersion(CODEC_V2)) {
            builder.field(CODEC_VERSION_FIELD.getPreferredName(), getCodecVersion());
            if (getCoordinationMetadata() != null) {
                builder.startObject(UPLOADED_COORDINATOR_METADATA.getPreferredName());
                getCoordinationMetadata().toXContent(builder, params);
                builder.endObject();
            }
            if (getSettingsMetadata() != null) {
                builder.startObject(UPLOADED_SETTINGS_METADATA.getPreferredName());
                getSettingsMetadata().toXContent(builder, params);
                builder.endObject();
            }
            if (getTemplatesMetadata() != null) {
                builder.startObject(UPLOADED_TEMPLATES_METADATA.getPreferredName());
                getTemplatesMetadata().toXContent(builder, params);
                builder.endObject();
            }
            builder.startObject(UPLOADED_CUSTOM_METADATA.getPreferredName());
            for (UploadedMetadataAttribute attribute : getCustomMetadataMap().values()) {
                attribute.toXContent(builder, params);
            }
            builder.endObject();
            builder.field(ROUTING_TABLE_VERSION_FIELD.getPreferredName(), getRoutingTableVersion());
            builder.startArray(INDICES_ROUTING_FIELD.getPreferredName());
            {
                for (UploadedIndexMetadata uploadedIndexMetadata : indicesRouting) {
                    builder.startObject();
                    uploadedIndexMetadata.toXContent(builder, params);
                    builder.endObject();
                }
            }
            builder.endArray();
            if (getDiscoveryNodesMetadata() != null) {
                builder.startObject(UPLOADED_DISCOVERY_NODES_METADATA.getPreferredName());
                getDiscoveryNodesMetadata().toXContent(builder, params);
                builder.endObject();
            }
            if (getClusterBlocksMetadata() != null) {
                builder.startObject(UPLOADED_CLUSTER_BLOCKS_METADATA.getPreferredName());
                getClusterBlocksMetadata().toXContent(builder, params);
                builder.endObject();
            }
            if (getTransientSettingsMetadata() != null) {
                builder.startObject(UPLOADED_TRANSIENT_SETTINGS_METADATA.getPreferredName());
                getTransientSettingsMetadata().toXContent(builder, params);
                builder.endObject();
            }
            if (getDiffManifest() != null) {
                builder.startObject(DIFF_MANIFEST.getPreferredName());
                getDiffManifest().toXContent(builder, params);
                builder.endObject();
            }
            builder.field(METADATA_VERSION.getPreferredName(), getMetadataVersion());
            if (getHashesOfConsistentSettings() != null) {
                builder.startObject(UPLOADED_HASHES_OF_CONSISTENT_SETTINGS_METADATA.getPreferredName());
                getHashesOfConsistentSettings().toXContent(builder, params);
                builder.endObject();
            }
            builder.startObject(UPLOADED_CLUSTER_STATE_CUSTOM_METADATA.getPreferredName());
            for (UploadedMetadataAttribute attribute : getClusterStateCustomMap().values()) {
                attribute.toXContent(builder, params);
            }
            builder.endObject();
        } else if (onOrAfterCodecVersion(CODEC_V1)) {
            builder.field(CODEC_VERSION_FIELD.getPreferredName(), getCodecVersion());
            builder.field(GLOBAL_METADATA_FIELD.getPreferredName(), getGlobalMetadataFileName());
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(clusterTerm);
        out.writeVLong(stateVersion);
        out.writeString(clusterUUID);
        out.writeString(stateUUID);
        out.writeInt(opensearchVersion.id);
        out.writeString(nodeId);
        out.writeBoolean(committed);
        out.writeCollection(indices);
        out.writeString(previousClusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        if (out.getVersion().onOrAfter(Version.V_2_15_0)) {
            out.writeInt(codecVersion);
            uploadedCoordinationMetadata.writeTo(out);
            uploadedSettingsMetadata.writeTo(out);
            uploadedTemplatesMetadata.writeTo(out);
            out.writeMap(uploadedCustomMetadataMap, StreamOutput::writeString, (o, v) -> v.writeTo(o));
            out.writeLong(routingTableVersion);
            out.writeCollection(indicesRouting);
            out.writeLong(metadataVersion);
            if (uploadedDiscoveryNodesMetadata != null) {
                out.writeBoolean(true);
                uploadedDiscoveryNodesMetadata.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            if (uploadedClusterBlocksMetadata != null) {
                out.writeBoolean(true);
                uploadedClusterBlocksMetadata.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            if (uploadedTransientSettingsMetadata != null) {
                out.writeBoolean(true);
                uploadedTransientSettingsMetadata.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            if (uploadedHashesOfConsistentSettings != null) {
                out.writeBoolean(true);
                uploadedHashesOfConsistentSettings.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            out.writeMap(uploadedClusterStateCustomMap, StreamOutput::writeString, (o, v) -> v.writeTo(o));
            if (diffManifest != null) {
                out.writeBoolean(true);
                diffManifest.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        } else if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeInt(codecVersion);
            out.writeString(globalMetadataFileName);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterMetadataManifest that = (ClusterMetadataManifest) o;
        return Objects.equals(indices, that.indices)
            && clusterTerm == that.clusterTerm
            && stateVersion == that.stateVersion
            && Objects.equals(clusterUUID, that.clusterUUID)
            && Objects.equals(stateUUID, that.stateUUID)
            && Objects.equals(opensearchVersion, that.opensearchVersion)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(committed, that.committed)
            && Objects.equals(previousClusterUUID, that.previousClusterUUID)
            && Objects.equals(clusterUUIDCommitted, that.clusterUUIDCommitted)
            && Objects.equals(globalMetadataFileName, that.globalMetadataFileName)
            && Objects.equals(codecVersion, that.codecVersion)
            && Objects.equals(routingTableVersion, that.routingTableVersion)
            && Objects.equals(indicesRouting, that.indicesRouting)
            && Objects.equals(uploadedCoordinationMetadata, that.uploadedCoordinationMetadata)
            && Objects.equals(uploadedSettingsMetadata, that.uploadedSettingsMetadata)
            && Objects.equals(uploadedTemplatesMetadata, that.uploadedTemplatesMetadata)
            && Objects.equals(uploadedCustomMetadataMap, that.uploadedCustomMetadataMap)
            && Objects.equals(metadataVersion, that.metadataVersion)
            && Objects.equals(uploadedDiscoveryNodesMetadata, that.uploadedDiscoveryNodesMetadata)
            && Objects.equals(uploadedClusterBlocksMetadata, that.uploadedClusterBlocksMetadata)
            && Objects.equals(uploadedTransientSettingsMetadata, that.uploadedTransientSettingsMetadata)
            && Objects.equals(uploadedHashesOfConsistentSettings, that.uploadedHashesOfConsistentSettings)
            && Objects.equals(uploadedClusterStateCustomMap, that.uploadedClusterStateCustomMap)
            && Objects.equals(diffManifest, that.diffManifest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            codecVersion,
            globalMetadataFileName,
            indices,
            clusterTerm,
            stateVersion,
            clusterUUID,
            stateUUID,
            opensearchVersion,
            nodeId,
            committed,
            previousClusterUUID,
            clusterUUIDCommitted,
            routingTableVersion,
            indicesRouting,
            uploadedCoordinationMetadata,
            uploadedSettingsMetadata,
            uploadedTemplatesMetadata,
            uploadedCustomMetadataMap,
            metadataVersion,
            uploadedDiscoveryNodesMetadata,
            uploadedClusterBlocksMetadata,
            uploadedTransientSettingsMetadata,
            uploadedHashesOfConsistentSettings,
            uploadedClusterStateCustomMap,
            diffManifest
        );
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    public boolean onOrAfterCodecVersion(int codecVersion) {
        return this.codecVersion >= codecVersion;
    }

    public static ClusterMetadataManifest fromXContentV0(XContentParser parser) throws IOException {
        return PARSER_V0.parse(parser, null);
    }

    public static ClusterMetadataManifest fromXContentV1(XContentParser parser) throws IOException {
        return PARSER_V1.parse(parser, null);
    }

    public static ClusterMetadataManifest fromXContentV2(XContentParser parser) throws IOException {
        return PARSER_V2.parse(parser, null);
    }

    public static ClusterMetadataManifest fromXContent(XContentParser parser) throws IOException {
        return CURRENT_PARSER.parse(parser, null);
    }

    /**
     * Builder for ClusterMetadataManifest
     *
     * @opensearch.internal
     */
    public static class Builder {

        private String globalMetadataFileName;
        private UploadedMetadataAttribute coordinationMetadata;
        private UploadedMetadataAttribute settingsMetadata;
        private UploadedMetadataAttribute templatesMetadata;
        private Map<String, UploadedMetadataAttribute> customMetadataMap;
        private int codecVersion;
        private List<UploadedIndexMetadata> indices;
        private long clusterTerm;
        private long stateVersion;
        private String clusterUUID;
        private String stateUUID;
        private Version opensearchVersion;
        private String nodeId;
        private String previousClusterUUID;
        private boolean committed;
        private boolean clusterUUIDCommitted;
        private long routingTableVersion;
        private List<UploadedIndexMetadata> indicesRouting;
        private long metadataVersion;
        private UploadedMetadataAttribute discoveryNodesMetadata;
        private UploadedMetadataAttribute clusterBlocksMetadata;
        private UploadedMetadataAttribute transientSettingsMetadata;
        private UploadedMetadataAttribute hashesOfConsistentSettings;
        private Map<String, UploadedMetadataAttribute> clusterStateCustomMetadataMap;
        private ClusterStateDiffManifest diffManifest;

        public Builder indices(List<UploadedIndexMetadata> indices) {
            this.indices = indices;
            return this;
        }

        public Builder routingTableVersion(long routingTableVersion) {
            this.routingTableVersion = routingTableVersion;
            return this;
        }

        public Builder indicesRouting(List<UploadedIndexMetadata> indicesRouting) {
            this.indicesRouting = indicesRouting;
            return this;
        }

        public Builder codecVersion(int codecVersion) {
            this.codecVersion = codecVersion;
            return this;
        }

        public Builder globalMetadataFileName(String globalMetadataFileName) {
            this.globalMetadataFileName = globalMetadataFileName;
            return this;
        }

        public Builder coordinationMetadata(UploadedMetadataAttribute coordinationMetadata) {
            this.coordinationMetadata = coordinationMetadata;
            return this;
        }

        public Builder settingMetadata(UploadedMetadataAttribute settingsMetadata) {
            this.settingsMetadata = settingsMetadata;
            return this;
        }

        public Builder templatesMetadata(UploadedMetadataAttribute templatesMetadata) {
            this.templatesMetadata = templatesMetadata;
            return this;
        }

        public Builder customMetadataMap(Map<String, UploadedMetadataAttribute> customMetadataMap) {
            this.customMetadataMap = customMetadataMap;
            return this;
        }

        public Builder put(String custom, UploadedMetadataAttribute customMetadata) {
            this.customMetadataMap.put(custom, customMetadata);
            return this;
        }

        public Builder clusterTerm(long clusterTerm) {
            this.clusterTerm = clusterTerm;
            return this;
        }

        public Builder stateVersion(long stateVersion) {
            this.stateVersion = stateVersion;
            return this;
        }

        public Builder clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
            return this;
        }

        public Builder stateUUID(String stateUUID) {
            this.stateUUID = stateUUID;
            return this;
        }

        public Builder opensearchVersion(Version opensearchVersion) {
            this.opensearchVersion = opensearchVersion;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder committed(boolean committed) {
            this.committed = committed;
            return this;
        }

        public List<UploadedIndexMetadata> getIndices() {
            return indices;
        }

        public List<UploadedIndexMetadata> getIndicesRouting() {
            return indicesRouting;
        }

        public Builder previousClusterUUID(String previousClusterUUID) {
            this.previousClusterUUID = previousClusterUUID;
            return this;
        }

        public Builder clusterUUIDCommitted(boolean clusterUUIDCommitted) {
            this.clusterUUIDCommitted = clusterUUIDCommitted;
            return this;
        }

        public Builder metadataVersion(long metadataVersion) {
            this.metadataVersion = metadataVersion;
            return this;
        }

        public Builder discoveryNodesMetadata(UploadedMetadataAttribute discoveryNodesMetadata) {
            this.discoveryNodesMetadata = discoveryNodesMetadata;
            return this;
        }

        public Builder clusterBlocksMetadata(UploadedMetadataAttribute clusterBlocksMetadata) {
            this.clusterBlocksMetadata = clusterBlocksMetadata;
            return this;
        }

        public Builder transientSettingsMetadata(UploadedMetadataAttribute settingsMetadata) {
            this.transientSettingsMetadata = settingsMetadata;
            return this;
        }

        public Builder hashesOfConsistentSettings(UploadedMetadataAttribute hashesOfConsistentSettings) {
            this.hashesOfConsistentSettings = hashesOfConsistentSettings;
            return this;
        }

        public Builder clusterStateCustomMetadataMap(Map<String, UploadedMetadataAttribute> clusterStateCustomMetadataMap) {
            this.clusterStateCustomMetadataMap = clusterStateCustomMetadataMap;
            return this;
        }

        public Builder diffManifest(ClusterStateDiffManifest diffManifest) {
            this.diffManifest = diffManifest;
            return this;
        }

        public Builder() {
            indices = new ArrayList<>();
            customMetadataMap = new HashMap<>();
            indicesRouting = new ArrayList<>();
            clusterStateCustomMetadataMap = new HashMap<>();
        }

        public Builder(ClusterMetadataManifest manifest) {
            this.clusterTerm = manifest.clusterTerm;
            this.stateVersion = manifest.stateVersion;
            this.clusterUUID = manifest.clusterUUID;
            this.stateUUID = manifest.stateUUID;
            this.opensearchVersion = manifest.opensearchVersion;
            this.nodeId = manifest.nodeId;
            this.committed = manifest.committed;
            this.globalMetadataFileName = manifest.globalMetadataFileName;
            this.coordinationMetadata = manifest.uploadedCoordinationMetadata;
            this.settingsMetadata = manifest.uploadedSettingsMetadata;
            this.templatesMetadata = manifest.uploadedTemplatesMetadata;
            this.customMetadataMap = manifest.uploadedCustomMetadataMap;
            this.codecVersion = manifest.codecVersion;
            this.indices = new ArrayList<>(manifest.indices);
            this.previousClusterUUID = manifest.previousClusterUUID;
            this.clusterUUIDCommitted = manifest.clusterUUIDCommitted;
            this.routingTableVersion = manifest.routingTableVersion;
            this.indicesRouting = new ArrayList<>(manifest.indicesRouting);
            this.discoveryNodesMetadata = manifest.uploadedDiscoveryNodesMetadata;
            this.clusterBlocksMetadata = manifest.uploadedClusterBlocksMetadata;
            this.transientSettingsMetadata = manifest.uploadedTransientSettingsMetadata;
            this.diffManifest = manifest.diffManifest;
            this.hashesOfConsistentSettings = manifest.uploadedHashesOfConsistentSettings;
            this.clusterStateCustomMetadataMap = manifest.uploadedClusterStateCustomMap;
        }

        public ClusterMetadataManifest build() {
            return new ClusterMetadataManifest(
                clusterTerm,
                stateVersion,
                clusterUUID,
                stateUUID,
                opensearchVersion,
                nodeId,
                committed,
                codecVersion,
                globalMetadataFileName,
                indices,
                previousClusterUUID,
                clusterUUIDCommitted,
                coordinationMetadata,
                settingsMetadata,
                templatesMetadata,
                customMetadataMap,
                routingTableVersion,
                indicesRouting,
                metadataVersion,
                discoveryNodesMetadata,
                clusterBlocksMetadata,
                transientSettingsMetadata,
                hashesOfConsistentSettings,
                clusterStateCustomMetadataMap,
                diffManifest
            );
        }

    }

    /**
     * Interface representing uploaded metadata
     */
    public interface UploadedMetadata {
        /**
         * Gets the component or part of the system this upload belongs to.
         *
         * @return A string identifying the component
         */
        String getComponent();

        /**
         * Gets the name of the file that was uploaded
         *
         * @return The name of the uploaded file as a string
         */
        String getUploadedFilename();
    }

    /**
     * Metadata for uploaded index metadata
     *
     * @opensearch.internal
     */
    public static class UploadedIndexMetadata implements UploadedMetadata, Writeable, ToXContentFragment {

        private static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
        private static final ParseField INDEX_UUID_FIELD = new ParseField("index_uuid");
        private static final ParseField UPLOADED_FILENAME_FIELD = new ParseField("uploaded_filename");
        private static final ParseField COMPONENT_PREFIX_FIELD = new ParseField("component_prefix");

        private static String indexName(Object[] fields) {
            return (String) fields[0];
        }

        private static String indexUUID(Object[] fields) {
            return (String) fields[1];
        }

        private static String uploadedFilename(Object[] fields) {
            return (String) fields[2];
        }

        private static String componentPrefix(Object[] fields) {
            return (String) fields[3];
        }

        private static final ConstructingObjectParser<UploadedIndexMetadata, Void> PARSER = new ConstructingObjectParser<>(
            "uploaded_index_metadata",
            fields -> new UploadedIndexMetadata(indexName(fields), indexUUID(fields), uploadedFilename(fields), componentPrefix(fields))
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_NAME_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_UUID_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), UPLOADED_FILENAME_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), COMPONENT_PREFIX_FIELD);
        }

        static final String COMPONENT_PREFIX = "index--";
        private final String componentPrefix;
        private final String indexName;
        private final String indexUUID;
        private final String uploadedFilename;

        public UploadedIndexMetadata(String indexName, String indexUUID, String uploadedFileName) {
            this(indexName, indexUUID, uploadedFileName, COMPONENT_PREFIX);
        }

        public UploadedIndexMetadata(String indexName, String indexUUID, String uploadedFileName, String componentPrefix) {
            this.componentPrefix = componentPrefix;
            this.indexName = indexName;
            this.indexUUID = indexUUID;
            this.uploadedFilename = uploadedFileName;
        }

        public UploadedIndexMetadata(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.indexUUID = in.readString();
            this.uploadedFilename = in.readString();
            this.componentPrefix = in.readString();
        }

        public String getUploadedFilePath() {
            return uploadedFilename;
        }

        @Override
        public String getComponent() {
            return componentPrefix + getIndexName();
        }

        public String getUploadedFilename() {
            return uploadedFilename;
        }

        public String getIndexName() {
            return indexName;
        }

        public String getIndexUUID() {
            return indexUUID;
        }

        public String getComponentPrefix() {
            return componentPrefix;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(INDEX_NAME_FIELD.getPreferredName(), getIndexName())
                .field(INDEX_UUID_FIELD.getPreferredName(), getIndexUUID())
                .field(UPLOADED_FILENAME_FIELD.getPreferredName(), getUploadedFilePath())
                .field(COMPONENT_PREFIX_FIELD.getPreferredName(), getComponentPrefix());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeString(indexUUID);
            out.writeString(uploadedFilename);
            out.writeString(componentPrefix);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final UploadedIndexMetadata that = (UploadedIndexMetadata) o;
            return Objects.equals(indexName, that.indexName)
                && Objects.equals(indexUUID, that.indexUUID)
                && Objects.equals(uploadedFilename, that.uploadedFilename)
                && Objects.equals(componentPrefix, that.componentPrefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, indexUUID, uploadedFilename, componentPrefix);
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }

        public static UploadedIndexMetadata fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Metadata for uploaded metadata attribute
     *
     * @opensearch.internal
     */
    public static class UploadedMetadataAttribute implements UploadedMetadata, Writeable, ToXContentFragment {
        private static final ParseField UPLOADED_FILENAME_FIELD = new ParseField("uploaded_filename");

        private static final ObjectParser.NamedObjectParser<UploadedMetadataAttribute, Void> PARSER;

        static {
            ConstructingObjectParser<UploadedMetadataAttribute, String> innerParser = new ConstructingObjectParser<>(
                "uploaded_metadata_attribute",
                true,
                (Object[] parsedObject, String name) -> {
                    String uploadedFilename = (String) parsedObject[0];
                    return new UploadedMetadataAttribute(name, uploadedFilename);
                }
            );
            innerParser.declareString(ConstructingObjectParser.constructorArg(), UPLOADED_FILENAME_FIELD);
            PARSER = ((p, c, name) -> innerParser.parse(p, name));
        }

        private final String attributeName;
        private final String uploadedFilename;

        public UploadedMetadataAttribute(String attributeName, String uploadedFilename) {
            this.attributeName = attributeName;
            this.uploadedFilename = uploadedFilename;
        }

        public UploadedMetadataAttribute(StreamInput in) throws IOException {
            this.attributeName = in.readString();
            this.uploadedFilename = in.readString();
        }

        public String getAttributeName() {
            return attributeName;
        }

        @Override
        public String getComponent() {
            return getAttributeName();
        }

        public String getUploadedFilename() {
            return uploadedFilename;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(attributeName);
            out.writeString(uploadedFilename);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject(getAttributeName())
                .field(UPLOADED_FILENAME_FIELD.getPreferredName(), getUploadedFilename())
                .endObject();
        }

        public static UploadedMetadataAttribute fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null, parser.currentName());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UploadedMetadataAttribute that = (UploadedMetadataAttribute) o;
            return Objects.equals(attributeName, that.attributeName) && Objects.equals(uploadedFilename, that.uploadedFilename);
        }

        @Override
        public int hashCode() {
            return Objects.hash(attributeName, uploadedFilename);
        }

        @Override
        public String toString() {
            return "UploadedMetadataAttribute{"
                + "attributeName='"
                + attributeName
                + '\''
                + ", uploadedFilename='"
                + uploadedFilename
                + '\''
                + '}';
        }
    }
}
