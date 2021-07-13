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
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A system index descriptor describes one or more system indices. It can match a number of indices using
 * a pattern. For system indices that are managed externally to Elasticsearch, this is enough. For system
 * indices that are managed internally to Elasticsearch, a descriptor can also include information for
 * creating the system index, upgrading its mappings, and creating an alias.
 */
public class SystemIndexDescriptor implements IndexPatternMatcher, Comparable<SystemIndexDescriptor> {
    /** A pattern, either with a wildcard or simple regex. Indices that match one of these patterns are considered system indices. */
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
    private final int indexFormat;

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

    private final boolean isNetNew;

    /**
     * The thread pools that actions will use to operate on this descriptor's system indices
     */
    private final ExecutorNames executorNames;

    /**
     * Creates a descriptor for system indices matching the supplied pattern. These indices will not be managed
     * by Elasticsearch internally.
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param description The name of the plugin responsible for this system index.
     */
    public SystemIndexDescriptor(String indexPattern, String description) {
        this(indexPattern, null, description, null, null, null, 0, null, null, Version.CURRENT.minimumCompatibilityVersion(),
            Type.INTERNAL_UNMANAGED, List.of(), List.of(), null, false);
    }

    /**
     * Creates a descriptor for system indices matching the supplied pattern. These indices will not be managed
     * by Elasticsearch internally.
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param description The name of the plugin responsible for this system index.
     * @param type The {@link Type} of system index
     * @param allowedElasticProductOrigins A list of allowed origin values that should be allowed access in the case of external system
     *                                     indices
     */
    public SystemIndexDescriptor(String indexPattern, String description, Type type, List<String> allowedElasticProductOrigins) {
        this(indexPattern, null, description, null, null, null, 0, null, null, Version.CURRENT.minimumCompatibilityVersion(), type,
            allowedElasticProductOrigins, List.of(), null, false);
    }

    /**
     * Creates a descriptor for system indices matching the supplied pattern. These indices will be managed
     * by Elasticsearch internally if mappings or settings are provided.
     *
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param primaryIndex The primary index name of this descriptor. Used when creating the system index for the first time.
     * @param description The name of the plugin responsible for this system index.
     * @param mappings The mappings to apply to this index when auto-creating, if appropriate
     * @param settings The settings to apply to this index when auto-creating, if appropriate
     * @param aliasName An alias for the index, or null
     * @param indexFormat A value for the `index.format` setting. Pass 0 or higher.
     * @param versionMetaKey a mapping key under <code>_meta</code> where a version can be found, which indicates the
    *                       Elasticsearch version when the index was created.
     * @param origin the client origin to use when creating this index.
     * @param minimumNodeVersion the minimum cluster node version required for this descriptor
     * @param type The {@link Type} of system index
     * @param allowedElasticProductOrigins A list of allowed origin values that should be allowed access in the case of external system
     *                                     indices
     * @param priorSystemIndexDescriptors A list of system index descriptors that describe the same index in a way that is compatible with
     *                                    older versions of Elasticsearch
     */
    SystemIndexDescriptor(
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
        boolean isNetNew
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
        if (type.isManaged()) {
            Objects.requireNonNull(settings, "Must supply settings for a managed system index");
            Strings.requireNonEmpty(mappings, "Must supply mappings for a managed system index");
            Strings.requireNonEmpty(primaryIndex, "Must supply primaryIndex for a managed system index");
            Strings.requireNonEmpty(versionMetaKey, "Must supply versionMetaKey for a managed system index");
            Strings.requireNonEmpty(origin, "Must supply origin for a managed system index");
            this.mappingVersion = extractVersionFromMappings(mappings, versionMetaKey);;
        } else {
            this.mappingVersion = null;
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
            // the rules for prior system index descriptors
            // 1. No values with the same minimum node version
            // 2. All prior system index descriptors must have a minimumNodeVersion before this one
            // 3. Prior system index descriptors may not have other prior system index descriptors
            //    to avoid multiple branches that need followed
            // 4. Must have same indexPattern, primaryIndex, and alias
            Set<Version> versions = new HashSet<>(priorSystemIndexDescriptors.size() + 1);
            versions.add(minimumNodeVersion);
            for (SystemIndexDescriptor prior : priorSystemIndexDescriptors) {
                if (versions.add(prior.minimumNodeVersion) == false) {
                    throw new IllegalArgumentException(prior + " has the same minimum node version as another descriptor");
                }
                if (prior.minimumNodeVersion.after(minimumNodeVersion)) {
                    throw new IllegalArgumentException(prior + " has minimum node version [" + prior.minimumNodeVersion +
                        "] which is after [" + minimumNodeVersion + "]");
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

        if (Objects.nonNull(settings) && settings.getAsBoolean(IndexMetadata.SETTING_INDEX_HIDDEN, false)) {
            throw new IllegalArgumentException("System indices cannot have " + IndexMetadata.SETTING_INDEX_HIDDEN +
                " set to true.");
        }
        this.settings = settings;
        this.indexFormat = indexFormat;
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
        this.executorNames = Objects.nonNull(executorNames)
            ? executorNames
            : ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS;
        this.isNetNew = isNetNew;
    }


    /**
     * @return The pattern of index names that this descriptor will be used for.
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
        ArrayList<String> matchingIndices = new ArrayList<>();
        metadata.indices().keysIt().forEachRemaining(indexName -> {
            if (matchesIndexPattern(indexName)) {
                matchingIndices.add(indexName);
            }
        });

        return Collections.unmodifiableList(matchingIndices);
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
        return mappings;
    }

    public Settings getSettings() {
        return settings;
    }

    public String getAliasName() {
        return aliasName;
    }

    public int getIndexFormat() {
        return this.indexFormat;
    }

    public String getVersionMetaKey() {
        return this.versionMetaKey;
    }

    public Version getMinimumNodeVersion() {
        return minimumNodeVersion;
    }

    public boolean isAutomaticallyManaged() {
        return type.isManaged();
    }

    public String getOrigin() {
        return this.origin;
    }

    public boolean hasDynamicMappings() {
        return this.hasDynamicMappings;
    }

    public boolean isExternal() {
        return type.isExternal();
    }

    public boolean isInternal() {
        return type.isInternal();
    }

    public List<String> getAllowedElasticProductOrigins() {
        return allowedElasticProductOrigins;
    }

    public boolean isNetNew() {
        return isNetNew;
    }

    public Version getMappingVersion() {
        if (type.isManaged() == false) {
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
        final Version actualMinimumVersion = priorSystemIndexDescriptors.isEmpty() ? minimumNodeVersion :
            priorSystemIndexDescriptors.get(priorSystemIndexDescriptors.size() - 1).minimumNodeVersion;
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

        public Builder setIndexFormat(int indexFormat) {
            this.indexFormat = indexFormat;
            return this;
        }

        public Builder setVersionMetaKey(String versionMetaKey) {
            this.versionMetaKey = versionMetaKey;
            return this;
        }

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

        public Builder setThreadPools(ExecutorNames executorNames) {
            this.executorNames = executorNames;
            return this;
        }

        public Builder setNetNew() {
            this.isNetNew = true;
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
                isNetNew
            );
        }
    }

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
        output = output.replaceAll("\\.", "\\.");
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
