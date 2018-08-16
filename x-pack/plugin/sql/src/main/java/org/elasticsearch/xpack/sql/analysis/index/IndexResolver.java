/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.index;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.Types;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;

public class IndexResolver {

    public enum IndexType {

        INDEX("BASE TABLE"),
        ALIAS("ALIAS"),
        // value for user types unrecognized
        UNKNOWN("UKNOWN");

        public static final EnumSet<IndexType> VALID = EnumSet.of(INDEX, ALIAS);

        private final String toSql;

        IndexType(String sql) {
            this.toSql = sql;
        }

        public String toSql() {
            return toSql;
        }

        public static IndexType from(String name) {
            if (name != null) {
                name = name.toUpperCase(Locale.ROOT);
                for (IndexType type : IndexType.VALID) {
                    if (type.toSql.equals(name)) {
                        return type;
                    }
                }
            }
            return IndexType.UNKNOWN;
        }
    }

    public static class IndexInfo {
        private final String name;
        private final IndexType type;

        public IndexInfo(String name, IndexType type) {
            this.name = name;
            this.type = type;
        }

        public String name() {
            return name;
        }

        public IndexType type() {
            return type;
        }
        
        @Override
        public String toString() {
            return name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            IndexResolver.IndexInfo other = (IndexResolver.IndexInfo) obj;
            return Objects.equals(name, other.name)
                    && Objects.equals(type, other.type);
        }
    }

    private final Client client;
    private final String clusterName;


    public IndexResolver(Client client, String clusterName) {
        this.client = client;
        this.clusterName = clusterName;
    }

    public String clusterName() {
        return clusterName;
    }

    /**
     * Resolves only the names, differentiating between indices and aliases.
     * This method is required since the other methods rely on mapping which is tied to an index (not an alias).
     */
    public void resolveNames(String indexWildcard, String javaRegex, EnumSet<IndexType> types, ActionListener<Set<IndexInfo>> listener) {

        // first get aliases (if specified)
        boolean retrieveAliases = CollectionUtils.isEmpty(types) || types.contains(IndexType.ALIAS);
        boolean retrieveIndices = CollectionUtils.isEmpty(types) || types.contains(IndexType.INDEX);

        if (retrieveAliases) {
            GetAliasesRequest aliasRequest = new GetAliasesRequest()
                    .local(true)
                    .aliases(indexWildcard)
                    .indicesOptions(IndicesOptions.lenientExpandOpen());
    
            client.admin().indices().getAliases(aliasRequest, ActionListener.wrap(aliases ->
                            resolveIndices(indexWildcard, javaRegex, aliases, retrieveIndices, listener),
                            ex -> {
                                // with security, two exception can be thrown:
                                // INFE - if no alias matches
                                // security exception is the user cannot access aliases
    
                                // in both cases, that is allowed and we continue with the indices request
                                if (ex instanceof IndexNotFoundException || ex instanceof ElasticsearchSecurityException) {
                                    resolveIndices(indexWildcard, javaRegex, null, retrieveIndices, listener);
                                } else {
                                    listener.onFailure(ex);
                                }
                            }));
        } else {
            resolveIndices(indexWildcard, javaRegex, null, retrieveIndices, listener);
        }
    }

    private void resolveIndices(String indexWildcard, String javaRegex, GetAliasesResponse aliases,
            boolean retrieveIndices, ActionListener<Set<IndexInfo>> listener) {

        if (retrieveIndices) {
            GetIndexRequest indexRequest = new GetIndexRequest()
                    .local(true)
                    .indices(indexWildcard)
                    .indicesOptions(IndicesOptions.lenientExpandOpen());
    
            client.admin().indices().getIndex(indexRequest,
                    ActionListener.wrap(indices -> filterResults(indexWildcard, javaRegex, aliases, indices, listener),
                            listener::onFailure));
        } else {
            filterResults(indexWildcard, javaRegex, aliases, null, listener);
        }
    }

    private void filterResults(String indexWildcard, String javaRegex, GetAliasesResponse aliases, GetIndexResponse indices,
            ActionListener<Set<IndexInfo>> listener) {
        
        // since the index name does not support ?, filter the results manually
        Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;

        Set<IndexInfo> result = new TreeSet<>(Comparator.comparing(IndexInfo::name));
        // filter aliases (if present)
        if (aliases != null) {
            for (ObjectCursor<List<AliasMetaData>> cursor : aliases.getAliases().values()) {
                for (AliasMetaData amd : cursor.value) {
                    String alias = amd.alias();
                    if (alias != null && (pattern == null || pattern.matcher(alias).matches())) {
                        result.add(new IndexInfo(alias, IndexType.ALIAS));
                    }
                }
            }
        }
        // filter indices (if present)
        String[] indicesNames = indices != null ? indices.indices() : null;
        if (indicesNames != null) {
            for (String indexName : indicesNames) {
                if (pattern == null || pattern.matcher(indexName).matches()) {
                    result.add(new IndexInfo(indexName, IndexType.INDEX));
                }
            }
        }

        listener.onResponse(result);
    }


    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     */
    public void resolveWithSameMapping(String indexWildcard, String javaRegex, ActionListener<IndexResolution> listener) {
        GetIndexRequest getIndexRequest = createGetIndexRequest(indexWildcard);
        client.admin().indices().getIndex(getIndexRequest, ActionListener.wrap(response -> {
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = response.getMappings();

            List<IndexResolution> resolutions;
            if (mappings.size() > 0) {
                resolutions = new ArrayList<>(mappings.size());
                Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;
                for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexMappings : mappings) {
                    String concreteIndex = indexMappings.key;
                    if (pattern == null || pattern.matcher(concreteIndex).matches()) {
                        resolutions.add(buildGetIndexResult(concreteIndex, concreteIndex, indexMappings.value));
                    }
                }
            } else {
                resolutions = emptyList();
            }

            listener.onResponse(merge(resolutions, indexWildcard));
        }, listener::onFailure));
    }

    static IndexResolution merge(List<IndexResolution> resolutions, String indexWildcard) {
        IndexResolution merged = null;
        for (IndexResolution resolution : resolutions) {
            // everything that follows gets compared
            if (!resolution.isValid()) {
                return resolution;
            }
            // initialize resolution on first run
            if (merged == null) {
                merged = resolution;
            }
            // need the same mapping across all resolutions
            if (!merged.get().mapping().equals(resolution.get().mapping())) {
                return IndexResolution.invalid(
                        "[" + indexWildcard + "] points to indices [" + merged.get().name() + "] "
                                + "and [" + resolution.get().name() + "] which have different mappings. "
                                + "When using multiple indices, the mappings must be identical.");
            }
        }
        if (merged != null) {
            // at this point, we are sure there's the same mapping across all (if that's the case) indices
            // to keep things simple, use the given pattern as index name
            merged = IndexResolution.valid(new EsIndex(indexWildcard, merged.get().mapping()));
        } else {
            merged = IndexResolution.notFound(indexWildcard);
        }
        return merged;
    }

    /**
     * Resolves a pattern to multiple, separate indices.
     */
    public void resolveAsSeparateMappings(String indexWildcard, String javaRegex, ActionListener<List<EsIndex>> listener) {
        GetIndexRequest getIndexRequest = createGetIndexRequest(indexWildcard);
        client.admin().indices().getIndex(getIndexRequest, ActionListener.wrap(getIndexResponse -> {
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getIndexResponse.getMappings();
            List<EsIndex> results = new ArrayList<>(mappings.size());
            Pattern pattern = javaRegex != null ? Pattern.compile(javaRegex) : null;
            for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexMappings : mappings) {
                /*
                 * We support wildcard expressions here, and it's only for commands that only perform the get index call.
                 * We can and simply have to use the concrete index name and show that to users.
                 * Get index against an alias with security enabled, where the user has only access to get mappings for the alias
                 * and not the concrete index: there is a well known information leak of the concrete index name in the response.
                 */
                String concreteIndex = indexMappings.key;
                if (pattern == null || pattern.matcher(concreteIndex).matches()) {
                    IndexResolution getIndexResult = buildGetIndexResult(concreteIndex, concreteIndex, indexMappings.value);
                    if (getIndexResult.isValid()) {
                        results.add(getIndexResult.get());
                    }
                }
            }
            results.sort(Comparator.comparing(EsIndex::name));
            listener.onResponse(results);
        }, listener::onFailure));
    }

    private static GetIndexRequest createGetIndexRequest(String index) {
        return new GetIndexRequest()
                .local(true)
                .indices(index)
                .features(Feature.MAPPINGS)
                //lenient because we throw our own errors looking at the response e.g. if something was not resolved
                //also because this way security doesn't throw authorization exceptions but rather honours ignore_unavailable
                .indicesOptions(IndicesOptions.lenientExpandOpen());
    }

    private static IndexResolution buildGetIndexResult(String concreteIndex, String indexOrAlias,
            ImmutableOpenMap<String, MappingMetaData> mappings) {

        // Make sure that the index contains only a single type
        MappingMetaData singleType = null;
        List<String> typeNames = null;
        for (ObjectObjectCursor<String, MappingMetaData> type : mappings) {
            //Default mappings are ignored as they are applied to each type. Each type alone holds all of its fields.
            if ("_default_".equals(type.key)) {
                continue;
            }
            if (singleType != null) {
                // There are more than one types
                if (typeNames == null) {
                    typeNames = new ArrayList<>();
                    typeNames.add(singleType.type());
                }
                typeNames.add(type.key);
            }
            singleType = type.value;
        }

        if (singleType == null) {
            return IndexResolution.invalid("[" + indexOrAlias + "] doesn't have any types so it is incompatible with sql");
        } else if (typeNames != null) {
            Collections.sort(typeNames);
            return IndexResolution.invalid(
                    "[" + indexOrAlias + "] contains more than one type " + typeNames + " so it is incompatible with sql");
        } else {
            try {
                Map<String, EsField> mapping = Types.fromEs(singleType.sourceAsMap());
                return IndexResolution.valid(new EsIndex(indexOrAlias, mapping));
            } catch (MappingException ex) {
                return IndexResolution.invalid(ex.getMessage());
            }
        }
    }
}