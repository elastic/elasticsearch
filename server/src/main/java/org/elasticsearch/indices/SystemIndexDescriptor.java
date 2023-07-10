/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.SystemIndexMetadataUpgradeService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.RegExp;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Uses a pattern string to define a protected space for indices belonging to a system feature, and, if needed, provides metadata for
 * managing indices that match the pattern.
 *
 * <p>Any index name that matches a descriptor’s index pattern belongs to the descriptor. For example, if a descriptor had a pattern of
 * {@code ".example-index-*"}, then indices named {@code ".example-index-1"}, {@code ".example-index-reindex"}, and {@code
 * ".example-index-old"} would all belong to the descriptor. If a node gains a new system index descriptor after an upgrade, then matching
 * indices will automatically be marked as system indices (see {@link SystemIndexMetadataUpgradeService}).
 *
 * <p>SystemIndexDescriptor index patterns must begin with a "{@code .}" character. Index patterns also have a "gotcha": the pattern
 * definitions may look like the standard Elasticsearch multi-target syntax but the underlying implementation is different. Index
 * patterns are actually defined in a mangled regex syntax where "{@code .}" is always interpreted as a literal character and "{@code *}"
 * is expanded to "{@code .*}". We don’t support date math or the "-" operator from the
 * {@link org.elasticsearch.cluster.metadata.IndexNameExpressionResolver} class. We intend for developers to use only the "{@code *}",
 * "{@code +}", "{@code ~}" (complement), "{@code (}", "{@code )}", and character class operators, but because of the implementation,
 * other regex operators probably work.
 *
 * <p>Sample index patterns that we want to handle:
 * <ol>
 *     <li>{@code .system-*} - covers all index names beginning with ".system-".
 *     <li>{@code .system-[0-9]+} - covers all index names beginning with ".system-" and containing only one or more numerals after that
 *     <li>{@code .system-~(other-*)} - covers all system indices beginning with ".system-", except for those beginning with
 *     ".system-other-"
 * </ol>
 *
 * <p>The descriptor defines which, if any, Elasticsearch products are expected to read or modify it with calls to the REST API.
 * Requests that do not include the correct product header should, in most cases, generate deprecation warnings. The exception is for
 * "net new" system index descriptors, described below.
 *
 * <p>The descriptor also provides names for the thread pools that Elasticsearch should use to read, search, or modify the descriptor’s
 * indices.
 *
 * <p>A SystemIndexDescriptor may be one of several types (see {@link SystemIndexDescriptor.Type}). The four types come from two different
 * distinctions. The first is between "internal" and "external" system indices. The second is between "managed and unmanaged" system
 * indices. The "internal/external" distinction is simple. Access to internal system indices via standard index APIs is deprecated,
 * and system features that use internal system indices should provide any necessary APIs for operating on their state. An "external"
 * system index, on the other hand, does not deprecate the use of standard index APIs.
 *
 * <p>The distinction between managed and unmanaged is simple in theory but not observed very well in our code. A "managed" system index
 * is one whose settings, mappings, and aliases are defined by the SystemIndexDescriptor and managed by Elasticsearch. Many of the
 * fields in this class, when added, were meant to be used only by managed system indices, and use of them should always be
 * conditional on whether the system index is managed or not. However, we have not consistently enforced this, so our code may have
 * inconsistent expectations about what fields will be defined for an unmanaged index. (In the future, we should refactor so that it
 * is clear which fields are ignored by unmanaged system indices.)
 *
 * <p>A managed system index defines a "primary index" which is intended to be the main write index for the descriptor. The current
 * behavior when creating a non-primary index is a little strange. A request to create a non-primary index with the Create Index
 * API will fail. (See <a href="https://github.com/elastic/elasticsearch/pull/86707">PR #86707</a>) However, auto-creating the index by
 * writing a document to it will succeed. (See <a href="https://github.com/elastic/elasticsearch/pull/77045">PR #77045</a>)
 *
 * <p>The mappings for managed system indices are automatically upgraded when all nodes in the cluster are compatible with the
 * descriptor's mappings. See {@link SystemIndexMappingUpdateService} for details.
 *
 * <p>We hope to remove the currently deprecated forms of access to system indices in a future release. A newly added system index with
 * no backwards-compatibility requirements may opt into our desired behavior by setting isNetNew to true. A "net new system index"
 * strictly enforces its allowed product origins, and cannot be accessed by any REST API request that lacks a correct product header.
 * A system index that is fully internal to Elasticsearch will not allow any product origins; such an index is fully "locked down,"
 * and in general can only be changed by restoring feature states from snapshots.
 */
public class SystemIndexDescriptor implements IndexPatternMatcher, Comparable<SystemIndexDescriptor> {

    public static final Settings DEFAULT_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build();

    /**
     * A pattern, either with a wildcard or simple regex. Indices that match one of these patterns are considered system indices.
     * Note that this pattern must not overlap with any other {@link SystemIndexDescriptor}s and must allow an alphanumeric suffix
     * (see {@link SystemIndices#UPGRADED_INDEX_SUFFIX} for the specific suffix that's checked) to ensure that there's a name within the
     * pattern we can use to create a new index when upgrading.
     * */
    private final String indexPattern;

    /**
     * For internally-managed indices, specifies the name of the concrete index to create and update. This is required
     * since the {@link #indexPattern} can match many indices.
     */
    private final String primaryIndex;

    /** A description of the index or indices */
    private final String description;

    /** Used to determine whether an index name matches the {@link #indexPattern} */
    private final CharacterRunAutomaton indexPatternAutomaton;

    /** For internally-managed indices, contains the index mappings JSON */
    private final String mappings;

    /** For internally-managed indices, contains the index settings */
    private final Settings settings;

    /** For internally-managed indices, an optional alias to create */
    private final String aliasName;

    /** For internally-managed indices, an optional {@link IndexMetadata#INDEX_FORMAT_SETTING} value to expect */
    private final IndexFormat indexFormat;

    /**
     * For internally-managed indices, specifies a key name under <code>_meta</code> in the index mappings
     * that contains the index's mappings' version.
     */
    private final String versionMetaKey;

    /** For internally-managed indices, specifies the origin to use when creating or updating the index */
    private final String origin;

    /** The minimum cluster node version required for this descriptor */
    private final Version minimumNodeVersion;

    /** Mapping version from the descriptor */
    private final Version mappingVersion;

    /** Whether there are dynamic fields in this descriptor's mappings */
    private final boolean hasDynamicMappings;

    /** The {@link Type} of system index this descriptor represents */
    private final Type type;

    /** A list of allowed product origins that may access an external system index */
    private final List<String> allowedElasticProductOrigins;

    /**
     * A list of prior system index descriptors that can be used when one or more data/master nodes is on a version lower than the
     * minimum supported version for this descriptor
     */
    private final List<SystemIndexDescriptor> priorSystemIndexDescriptors;

    /**
     * A system index that is <em>not</em> net-new allows deprecated access from API requests, but issues warnings. A net-new system
     * index has the desired future behavior of only being accessible by requests with the correct product origin header.
     */
    private final boolean isNetNew;

    /**
     * We typically don't want to apply user defined templates on system indices, since they may have unexpected
     * behaviour when upgrading Elasticsearch versions. Currently, only the .kibana_ indices use templates, so we
     * are making this property by default as false.
     */
    private final boolean allowsTemplates;

    /**
     * The thread pools that actions will use to operate on this descriptor's system indices
     */
    private final ExecutorNames executorNames;

    /**
     * Creates a descriptor for system indices matching the supplied pattern. These indices will be managed
     * by Elasticsearch internally if mappings or settings are provided.
     *
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character, must not
     *                            overlap with any other descriptor patterns, and must allow a suffix (see note on
     *                            {@link SystemIndexDescriptor#indexPattern} for details).
     * @param primaryIndex The primary index name of this descriptor. Used when creating the system index for the first time.
     * @param description The name of the plugin responsible for this system index.
     * @param mappings The mappings to apply to this index when auto-creating, if appropriate
     * @param settings The settings to apply to this index when auto-creating, if appropriate
     * @param aliasName An alias for the index, or null
     * @param indexFormat A value for the `index.format` setting. Pass 0 or higher.
     * @param versionMetaKey a mapping key under <code>_meta</code> where a version can be found, which indicates the
    *                       Elasticsearch version when the index was created.
     * @param origin the client origin to use when creating this index. Internal system indices must not provide an origin, while external
     *               system indices must do so.
     * @param minimumNodeVersion the minimum cluster node version required for this descriptor
     * @param type The {@link Type} of system index
     * @param allowedElasticProductOrigins A list of allowed origin values that should be allowed access in the case of external system
     *                                     indices
     * @param priorSystemIndexDescriptors A list of system index descriptors that describe the same index in a way that is compatible with
     *                                    older versions of Elasticsearch
     * @param allowsTemplates if this system index descriptor allows templates to affect its settings (e.g. .kibana_ indices)
     */
    protected SystemIndexDescriptor(
        String indexPattern,
        String primaryIndex,
        String description,
        String mappings,
        Settings settings,
        String aliasName,
        int indexFormat,
        String versionMetaKey,
        String origin,
        Version minimumNodeVersion,
        Type type,
        List<String> allowedElasticProductOrigins,
        List<SystemIndexDescriptor> priorSystemIndexDescriptors,
        ExecutorNames executorNames,
        boolean isNetNew,
        boolean allowsTemplates
    ) {
        Objects.requireNonNull(indexPattern, "system index pattern must not be null");
        if (indexPattern.length() < 2) {
            throw new IllegalArgumentException(
                "system index pattern provided as [" + indexPattern + "] but must at least 2 characters in length"
            );
        }
        if (indexPattern.charAt(0) != '.') {
            throw new IllegalArgumentException(
                "system index pattern provided as [" + indexPattern + "] but must start with the character [.]"
            );
        }
        if (indexPattern.charAt(1) == '*') {
            throw new IllegalArgumentException(
                "system index pattern provided as ["
                    + indexPattern
                    + "] but must not start with the character sequence [.*] to prevent conflicts"
            );
        }

        if (primaryIndex != null) {
            if (primaryIndex.charAt(0) != '.') {
                throw new IllegalArgumentException(
                    "system primary index provided as [" + primaryIndex + "] but must start with the character [.]"
                );
            }
            if (primaryIndex.matches("^\\.[\\w-]+$") == false) {
                throw new IllegalArgumentException(
                    "system primary index provided as [" + primaryIndex + "] but cannot contain special characters or patterns"
                );
            }
        }

        if (indexFormat < 0) {
            throw new IllegalArgumentException("Index format cannot be negative");
        }

        Strings.requireNonEmpty(indexPattern, "indexPattern must be supplied");

        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(indexFormat);

        if (type.isManaged()) {
            Objects.requireNonNull(settings, "Must supply settings for a managed system index");
            Strings.requireNonEmpty(mappings, "Must supply mappings for a managed system index");
            Strings.requireNonEmpty(primaryIndex, "Must supply primaryIndex for a managed system index");
            // todo: index formats as static constants?
            this.indexFormat = new IndexFormat(indexFormat, Integer.toString(getDescriptorHash(mappings, settings)));
            Objects.requireNonNull(this.indexFormat.hash, "Must supply index format hash for a managed system index");
            Strings.requireNonEmpty(versionMetaKey, "Must supply versionMetaKey for a managed system index");
            Strings.requireNonEmpty(origin, "Must supply origin for a managed system index");
            if (settings.getAsInt(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 0) != indexFormat) {
                throw new IllegalArgumentException("Descriptor index format does not match index format in managed settings");
            }
            this.mappingVersion = extractVersionFromMappings(mappings, versionMetaKey);
        } else {
            assert Objects.isNull(settings) : "Unmanaged index descriptors should not have settings";
            assert Objects.isNull(mappings) : "Unmanaged index descriptors should not have mappings";
            assert Objects.isNull(primaryIndex) : "Unmanaged index descriptors should not have a primary index";
            assert Objects.isNull(versionMetaKey) : "Unmanaged index descriptors should not have a version meta key";
            this.mappingVersion = null;
            this.indexFormat = new IndexFormat(indexFormat, null);
        }

        Objects.requireNonNull(allowedElasticProductOrigins, "allowedProductOrigins must not be null");
        if (type.isInternal() && allowedElasticProductOrigins.isEmpty() == false) {
            throw new IllegalArgumentException("Allowed origins are not valid for internal system indices");
        } else if (type.isExternal() && allowedElasticProductOrigins.isEmpty()) {
            throw new IllegalArgumentException("External system indices without allowed products is not a valid combination");
        }

        Objects.requireNonNull(minimumNodeVersion, "minimumNodeVersion must be provided!");
        Objects.requireNonNull(priorSystemIndexDescriptors, "priorSystemIndexDescriptors must not be null");
        if (priorSystemIndexDescriptors.isEmpty() == false) {
            // TODO[wrb]: remove Version from prior system index descriptor system - bwc concerns?
            // the rules for prior system index descriptors
            // 1. No values with the same minimum node version or index format
            // 2. All prior system index descriptors must have a minimumNodeVersion and index format before this one
            // 3. Prior system index descriptors may not have other prior system index descriptors
            // to avoid multiple branches that need followed
            // 4. Must have same indexPattern, primaryIndex, and alias
            Set<Version> versions = Sets.newHashSetWithExpectedSize(priorSystemIndexDescriptors.size() + 1);
            versions.add(minimumNodeVersion);
            Set<Integer> indexFormatVersions = Sets.newHashSetWithExpectedSize(priorSystemIndexDescriptors.size() + 1);
            indexFormatVersions.add(indexFormat);
            for (SystemIndexDescriptor prior : priorSystemIndexDescriptors) {
                if (versions.add(prior.minimumNodeVersion) == false) {
                    throw new IllegalArgumentException(prior + " has the same minimum node version as another descriptor");
                }
                if (prior.minimumNodeVersion.after(minimumNodeVersion)) {
                    throw new IllegalArgumentException(
                        prior + " has minimum node version [" + prior.minimumNodeVersion + "] which is after [" + minimumNodeVersion + "]"
                    );
                }
                if (indexFormatVersions.add(prior.indexFormat.indexFormat) == false) {
                    throw new IllegalArgumentException(prior + " has the same index format version as another descriptor");
                }
                if (prior.indexFormat.indexFormat > indexFormat) {
                    throw new IllegalArgumentException(
                        prior + " has index format [" + prior.indexFormat + "] which is after [" + indexFormat + "]"
                    );
                }
                if (prior.priorSystemIndexDescriptors.isEmpty() == false) {
                    throw new IllegalArgumentException(prior + " has its own prior descriptors but only a depth of 1 is allowed");
                }
                if (prior.indexPattern.equals(indexPattern) == false) {
                    throw new IllegalArgumentException("index pattern must be the same");
                }
                if (prior.primaryIndex.equals(primaryIndex) == false) {
                    throw new IllegalArgumentException("primary index must be the same");
                }
                if (prior.aliasName.equals(aliasName) == false) {
                    throw new IllegalArgumentException("alias name must be the same");
                }
            }
        }

        if (Objects.nonNull(executorNames)) {
            if (ThreadPool.THREAD_POOL_TYPES.containsKey(executorNames.threadPoolForGet()) == false) {
                throw new IllegalArgumentException(executorNames.threadPoolForGet() + " is not a valid thread pool");
            }
            if (ThreadPool.THREAD_POOL_TYPES.containsKey(executorNames.threadPoolForSearch()) == false) {
                throw new IllegalArgumentException(executorNames.threadPoolForGet() + " is not a valid thread pool");
            }
            if (ThreadPool.THREAD_POOL_TYPES.containsKey(executorNames.threadPoolForWrite()) == false) {
                throw new IllegalArgumentException(executorNames.threadPoolForGet() + " is not a valid thread pool");
            }
        }

        this.indexPattern = indexPattern;
        this.primaryIndex = primaryIndex;
        this.aliasName = aliasName;

        final Automaton automaton = buildAutomaton(indexPattern, aliasName);
        this.indexPatternAutomaton = new CharacterRunAutomaton(automaton);
        if (primaryIndex != null && indexPatternAutomaton.run(primaryIndex) == false) {
            throw new IllegalArgumentException("primary index does not match the index pattern!");
        }

        this.description = description;
        this.mappings = mappings;

        settings = Objects.isNull(settings) ? Settings.EMPTY : settings;

        if (settings.hasValue(IndexMetadata.SETTING_INDEX_HIDDEN) == false) {
            settings = Settings.builder().put(settings).put(DEFAULT_SETTINGS).build();
        }

        if (settings.getAsBoolean(IndexMetadata.SETTING_INDEX_HIDDEN, false)) {
            this.settings = settings;
        } else {
            throw new IllegalArgumentException("System indices must have " + IndexMetadata.SETTING_INDEX_HIDDEN + " set to true.");
        }
        this.versionMetaKey = versionMetaKey;
        this.origin = origin;
        this.minimumNodeVersion = minimumNodeVersion;
        this.type = type;
        this.allowedElasticProductOrigins = allowedElasticProductOrigins;
        this.hasDynamicMappings = this.mappings != null
            && findDynamicMapping(XContentHelper.convertToMap(JsonXContent.jsonXContent, mappings, false));

        final List<SystemIndexDescriptor> sortedPriorSystemIndexDescriptors;
        if (priorSystemIndexDescriptors.isEmpty() || priorSystemIndexDescriptors.size() == 1) {
            sortedPriorSystemIndexDescriptors = List.copyOf(priorSystemIndexDescriptors);
        } else {
            List<SystemIndexDescriptor> copy = new ArrayList<>(priorSystemIndexDescriptors);
            Collections.sort(copy);
            sortedPriorSystemIndexDescriptors = List.copyOf(copy);
        }
        this.priorSystemIndexDescriptors = sortedPriorSystemIndexDescriptors;
        this.executorNames = Objects.nonNull(executorNames) ? executorNames : ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS;
        this.isNetNew = isNetNew;
        this.allowsTemplates = allowsTemplates;
    }

    /**
     * @return The pattern of index names that this descriptor will be used for. Must start with a '.' character, must not
     *         overlap with any other descriptor patterns, and must allow a suffix (see note on
     *         {@link SystemIndexDescriptor#indexPattern} for details).
     */
    @Override
    public String getIndexPattern() {
        return indexPattern;
    }

    /**
     * @return The concrete name of an index being managed internally to Elasticsearch. Will be {@code null}
     * for indices managed externally to Elasticsearch.
     */
    public String getPrimaryIndex() {
        assert isAutomaticallyManaged() : "Unmanaged indices should not have a primary index";
        return primaryIndex;
    }

    /**
     * Checks whether an index name matches the system index name pattern for this descriptor.
     * @param index The index name to be checked against the index pattern given at construction time.
     * @return True if the name matches the pattern, false otherwise.
     */
    public boolean matchesIndexPattern(String index) {
        return indexPatternAutomaton.run(index);
    }

    /**
     * Retrieves a list of all indices which match this descriptor's pattern.
     *
     * This cannot be done via {@link org.elasticsearch.cluster.metadata.IndexNameExpressionResolver} because that class can only handle
     * simple wildcard expressions, but system index name patterns may use full Lucene regular expression syntax,
     *
     * @param metadata The current metadata to get the list of matching indices from
     * @return A list of index names that match this descriptor
     */
    @Override
    public List<String> getMatchingIndices(Metadata metadata) {
        return metadata.indices().keySet().stream().filter(this::matchesIndexPattern).toList();
    }

    /**
     * @return A short description of the purpose of this system index.
     */
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "SystemIndexDescriptor[pattern=[" + indexPattern + "], description=[" + description + "], aliasName=[" + aliasName + "]]";
    }

    public String getMappings() {
        assert isAutomaticallyManaged() : "Do not request mappings for unmanaged system indices";
        return mappings;
    }

    public Settings getSettings() {
        assert isAutomaticallyManaged() : "Do not request settings for unmanaged system indices";
        return settings;
    }

    public String getAliasName() {
        return aliasName;
    }

    public int getIndexFormat() {
        assert isAutomaticallyManaged() : "Do not request index format for unmanaged system indices";
        return this.indexFormat.indexFormat;
    }

    public String getVersionMetaKey() {
        assert isAutomaticallyManaged() : "Do not request version meta keys for unmanaged system indices";
        return this.versionMetaKey;
    }

    public Version getMinimumNodeVersion() {
        assert isAutomaticallyManaged() : "Do not request version minimum node version for unmanaged system indices";
        return minimumNodeVersion;
    }

    public boolean isAutomaticallyManaged() {
        return type.isManaged();
    }

    /**
     * Get an origin string suitable for use in an {@link org.elasticsearch.client.internal.OriginSettingClient}. See
     * {@link Builder#setOrigin(String)} for more information.
     *
     * @return an origin string to use for sub-requests
     */
    public String getOrigin() {
        // TODO[wrb]: most unmanaged system indices do not set origins; could we assert on that here?
        return this.origin;
    }

    public boolean hasDynamicMappings() {
        assert isAutomaticallyManaged() : "Do not check mapping properties for unmanaged system indices";
        return this.hasDynamicMappings;
    }

    public boolean isExternal() {
        return type.isExternal();
    }

    public boolean isInternal() {
        return type.isInternal();
    }

    /**
     * Requests from these products, if made with the proper security credentials, are allowed non-deprecated access to this descriptor's
     * indices. (Product names may be specified in requests with the
     * {@link org.elasticsearch.tasks.Task#X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER}).
     * @return A list of product names.
     */
    public List<String> getAllowedElasticProductOrigins() {
        return allowedElasticProductOrigins;
    }

    public boolean isNetNew() {
        return isNetNew;
    }

    public boolean allowsTemplates() {
        return allowsTemplates;
    }

    public Version getMappingVersion() {
        if (isAutomaticallyManaged() == false) {
            throw new IllegalStateException(this + " is not managed so there are no mappings or version");
        }
        return mappingVersion;
    }

    /**
     * Gets a standardized message when the node contains a data or master node whose version is less
     * than that of the minimum supported version of this descriptor and its prior descriptors.
     *
     * @param cause the action being attempted that triggered the check. Used in the error message.
     * @return the standardized error message
     */
    public String getMinimumNodeVersionMessage(String cause) {
        Objects.requireNonNull(cause);
        final Version actualMinimumVersion = priorSystemIndexDescriptors.isEmpty()
            ? minimumNodeVersion
            : priorSystemIndexDescriptors.get(priorSystemIndexDescriptors.size() - 1).minimumNodeVersion;
        return String.format(
            Locale.ROOT,
            "[%s] failed - system index [%s] requires all data and master nodes to be at least version [%s]",
            cause,
            this.getPrimaryIndex(),
            actualMinimumVersion
        );
    }

    /**
     * Finds the descriptor that can be used within this cluster, by comparing the supplied minimum
     * node version to this descriptor's minimum version and the prior descriptors minimum version.
     *
     * @param version the lower node version in the cluster
     * @return <code>null</code> if the lowest node version is lower than the minimum version in this descriptor,
     * or the appropriate descriptor if the supplied version is acceptable.
     */
    public SystemIndexDescriptor getDescriptorCompatibleWith(Version version) {
        if (minimumNodeVersion.onOrBefore(version)) {
            return this;
        }
        for (SystemIndexDescriptor prior : priorSystemIndexDescriptors) {
            if (version.onOrAfter(prior.minimumNodeVersion)) {
                return prior;
            }
        }
        return null;
    }

    public static int getDescriptorHash(String mappings, Settings settings) {
        Map<String, Object> convertedMappings = XContentHelper.convertToMap(XContentType.JSON.xContent(), mappings, false);
        return Objects.hash(convertedMappings, settings);
    }

    /**
     * @return The names of thread pools that should be used for operations on this
     *    system index.
     */
    public ExecutorNames getThreadPoolNames() {
        return this.executorNames;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int compareTo(SystemIndexDescriptor other) {
        return minimumNodeVersion.compareTo(other.minimumNodeVersion) * -1;
    }

    /**
     * The specific type of system index that this descriptor represents. System indices can be one of four defined types; the type is used
     * to control behavior. Elasticsearch itself and plugins have system indices that are necessary for their features;
     * these system indices are referred to as internal system indices. System indices can also belong to features outside of Elasticsearch
     * that may be part of other Elastic stack components. These are external system indices as the intent is for these to be accessed via
     * normal APIs with a special value.
     *
     * Within both internal and external system indices, there are two sub-types. The first are those that are managed by Elasticsearch and
     * will have mappings/settings changed as the cluster itself is upgraded. The second are those managed by the owning applications code
     * and for those Elasticsearch will not perform any updates.
     *
     * Internal system indices are almost always managed indices that Elasticsearch manages, but there are cases where the component of
     * Elasticsearch will need to manage the system indices itself.
     */
    public enum Type {
        INTERNAL_MANAGED(false, true),
        INTERNAL_UNMANAGED(false, false),
        EXTERNAL_MANAGED(true, true),
        EXTERNAL_UNMANAGED(true, false);

        private final boolean external;
        private final boolean managed;

        Type(boolean external, boolean managed) {
            this.external = external;
            this.managed = managed;
        }

        public boolean isExternal() {
            return external;
        }

        public boolean isManaged() {
            return managed;
        }

        public boolean isInternal() {
            return external == false;
        }
    }

    /**
     * Provides a fluent API for building a {@link SystemIndexDescriptor}. Validation still happens in that class.
     */
    public static class Builder {
        private String indexPattern;
        private String primaryIndex;
        private String description;
        private String mappings = null;
        private Settings settings = null;
        private String aliasName = null;
        private int indexFormat = 0;
        private String versionMetaKey = null;
        private String origin = null;
        private Version minimumNodeVersion = Version.CURRENT.minimumCompatibilityVersion();
        private Type type = Type.INTERNAL_MANAGED;
        private List<String> allowedElasticProductOrigins = List.of();
        private List<SystemIndexDescriptor> priorSystemIndexDescriptors = List.of();
        private ExecutorNames executorNames;
        private boolean isNetNew = false;
        private boolean allowsTemplates = false;

        private Builder() {}

        public Builder setIndexPattern(String indexPattern) {
            this.indexPattern = indexPattern;
            return this;
        }

        public Builder setPrimaryIndex(String primaryIndex) {
            this.primaryIndex = primaryIndex;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setMappings(XContentBuilder mappingsBuilder) {
            mappings = mappingsBuilder == null ? null : Strings.toString(mappingsBuilder);
            return this;
        }

        public Builder setMappings(String mappings) {
            this.mappings = mappings;
            return this;
        }

        public Builder setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder setAliasName(String aliasName) {
            this.aliasName = aliasName;
            return this;
        }

        // TODO[wrb]: index format usage
        // * security system indices
        // * watcher
        // * tests
        public Builder setIndexFormat(int indexFormat) {
            this.indexFormat = indexFormat;
            return this;
        }

        public Builder setVersionMetaKey(String versionMetaKey) {
            this.versionMetaKey = versionMetaKey;
            return this;
        }

        /**
         * Sometimes a system operation will need to dispatch sub-actions. A product origin string will tell the system which component
         * generated the sub-action. Internal system indices must not provide an origin, since they are supposed to reject access from
         * outside the system. External system indices, on the other hand, must provide an origin. See
         * {@link org.elasticsearch.client.internal.OriginSettingClient} for more information.
         * @param origin the client origin to use when creating this index.
         * @return a {@link Builder} object
         */
        public Builder setOrigin(String origin) {
            this.origin = origin;
            return this;
        }

        public Builder setMinimumNodeVersion(Version version) {
            this.minimumNodeVersion = version;
            return this;
        }

        public Builder setType(Type type) {
            this.type = type;
            return this;
        }

        public Builder setAllowedElasticProductOrigins(List<String> allowedElasticProductOrigins) {
            this.allowedElasticProductOrigins = allowedElasticProductOrigins;
            return this;
        }

        public Builder setPriorSystemIndexDescriptors(List<SystemIndexDescriptor> priorSystemIndexDescriptors) {
            this.priorSystemIndexDescriptors = priorSystemIndexDescriptors;
            return this;
        }

        public Builder setThreadPools(ExecutorNames threadPoolExecutorNames) {
            this.executorNames = threadPoolExecutorNames;
            return this;
        }

        public Builder setNetNew() {
            this.isNetNew = true;
            return this;
        }

        public Builder setAllowsTemplates() {
            this.allowsTemplates = true;
            return this;
        }

        /**
         * Builds a {@link SystemIndexDescriptor} using the fields supplied to this builder.
         * @return a populated descriptor.
         */
        public SystemIndexDescriptor build() {
            return new SystemIndexDescriptor(
                indexPattern,
                primaryIndex,
                description,
                mappings,
                settings,
                aliasName,
                indexFormat,
                versionMetaKey,
                origin,
                minimumNodeVersion,
                type,
                allowedElasticProductOrigins,
                priorSystemIndexDescriptors,
                executorNames,
                isNetNew,
                allowsTemplates
            );
        }
    }

    public record IndexFormat(int indexFormat, String hash) {}

    /**
     * Builds an automaton for matching index names against this descriptor's index pattern.
     * If this descriptor has an alias name, the automaton will also try to match against
     * the alias as well.
     */
    static Automaton buildAutomaton(String pattern, String alias) {
        final String patternAsRegex = patternToRegex(pattern);
        final String aliasAsRegex = alias == null ? null : patternToRegex(alias);

        final Automaton patternAutomaton = new RegExp(patternAsRegex).toAutomaton();

        if (aliasAsRegex == null) {
            return patternAutomaton;
        }

        final Automaton aliasAutomaton = new RegExp(aliasAsRegex).toAutomaton();

        return Operations.union(patternAutomaton, aliasAutomaton);
    }

    /**
     * Translate a simple string pattern into a regular expression, suitable for creating a
     * {@link RegExp} instance. This exists because although
     * {@link org.elasticsearch.common.regex.Regex#simpleMatchToAutomaton(String)} is useful
     * for simple patterns, it doesn't support character ranges.
     *
     * @param input the string to translate
     * @return the translate string
     */
    private static String patternToRegex(String input) {
        String output = input;
        output = output.replaceAll("\\.", "\\\\.");
        output = output.replaceAll("\\*", ".*");
        return output;
    }

    /**
     * Recursively searches for <code>dynamic: true</code> in the supplies mappings
     * @param map a parsed fragment of an index's mappings
     * @return whether the fragment contains a dynamic mapping
     */
    @SuppressWarnings("unchecked")
    static boolean findDynamicMapping(Map<String, Object> map) {
        if (map == null) {
            return false;
        }

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            if (key.equals("dynamic") && (value instanceof Boolean) && ((Boolean) value)) {
                return true;
            }

            if (value instanceof Map) {
                if (findDynamicMapping((Map<String, Object>) value)) {
                    return true;
                }
            }
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    private static Version extractVersionFromMappings(String mappings, String versionMetaKey) {
        final Map<String, Object> mappingsMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), mappings, false);
        final Map<String, Object> doc = (Map<String, Object>) mappingsMap.get("_doc");
        final Map<String, Object> meta;
        if (doc == null) {
            meta = (Map<String, Object>) mappingsMap.get("_meta");
        } else {
            meta = (Map<String, Object>) doc.get("_meta");
        }
        if (meta == null) {
            throw new IllegalStateException("mappings do not have _meta field");
        }
        final String value = (String) meta.get(versionMetaKey);
        if (value == null) {
            throw new IllegalArgumentException("mappings do not have a version in _meta." + versionMetaKey);
        }
        return Version.fromString(value);
    }

}
