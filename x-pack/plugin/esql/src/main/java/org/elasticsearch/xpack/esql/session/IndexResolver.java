/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.SupportedVersion;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;

public class IndexResolver {
    private static Logger LOGGER = LogManager.getLogger(IndexResolver.class);

    public static final Set<String> ALL_FIELDS = Set.of("*");
    public static final Set<String> INDEX_METADATA_FIELD = Set.of(MetadataAttribute.INDEX);
    public static final String UNMAPPED = "unmapped";

    public static final IndicesOptions FIELD_CAPS_INDICES_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            IndicesOptions.WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(false)
                .includeHidden(false)
                .allowEmptyExpressions(true)
                .resolveAliases(true)
        )
        .gatekeeperOptions(
            IndicesOptions.GatekeeperOptions.builder().ignoreThrottled(true).allowClosedIndices(true).allowAliasToMultipleIndices(true)
        )
        .build();

    private final Client client;

    public IndexResolver(Client client) {
        this.client = client;
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping.
     */
    public void resolveIndices(
        String indexWildcard,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        ActionListener<IndexResolution> listener
    ) {
        resolveIndicesVersioned(
            indexWildcard,
            fieldNames,
            requestFilter,
            includeAllDimensions,
            useAggregateMetricDoubleWhenNotSupported,
            useDenseVectorWhenNotSupported,
            listener.map(Versioned::inner)
        );
    }

    /**
     * Resolves a pattern to one (potentially compound meaning that spawns multiple indices) mapping. Also retrieves the minimum transport
     * version available in the cluster (and remotes).
     */
    public void resolveIndicesVersioned(
        String indexWildcard,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        client.execute(
            EsqlResolveFieldsAction.TYPE,
            createFieldCapsRequest(indexWildcard, fieldNames, requestFilter, includeAllDimensions),
            listener.delegateFailureAndWrap((l, response) -> {
                FieldsInfo info = new FieldsInfo(
                    response.caps(),
                    response.caps().minTransportVersion(),
                    Build.current().isSnapshot(),
                    useAggregateMetricDoubleWhenNotSupported,
                    useDenseVectorWhenNotSupported
                );
                LOGGER.debug("minimum transport version {} {}", response.caps().minTransportVersion(), info.effectiveMinTransportVersion());
                l.onResponse(new Versioned<>(mergedMappings(indexWildcard, info), info.effectiveMinTransportVersion()));
            })
        );
    }

    /**
     * Information for resolving a field.
     * @param caps {@link FieldCapabilitiesResponse} from all indices involved in the query
     * @param minTransportVersion The minimum {@link TransportVersion} of any node that <strong>might</strong> receive the request.
     *                            More precisely, it's the minimum transport version of ALL nodes in ALL the clusters that the query
     *                            is targeting. It doesn't matter if the node is a data node or an ML node or a unicorn, it's transport
     *                            version counts. BUT if the query doesn't dispatch to that cluster AT ALL, we don't count the versions
     *                            of any nodes in that cluster.
     *                            <p>
     *                                If this is {@code null} then one of the nodes is before
     *                                {@link EsqlResolveFieldsResponse#RESOLVE_FIELDS_RESPONSE_CREATED_TV} but we have no idea how early
     *                                it is. Could be back in {@code 8.19.0}.
     *                            </p>
     * @param currentBuildIsSnapshot is the current build a snapshot? Note: This is always {@code Build.current().isSnapshot()} in
     *                               production but tests need more control
     * @param useAggregateMetricDoubleWhenNotSupported does the query itself force us to use {@code aggregate_metric_double} fields
     *                                                 even if the remotes don't report that they support the type? This exists because
     *                                                 some remotes <strong>do</strong> support {@code aggregate_metric_double} without
     *                                                 reporting that they do. And, for a while, we used the query itself to opt into
     *                                                 reading these fields.
     * @param useDenseVectorWhenNotSupported does the query itself force us to use {@code dense_vector} fields even if the remotes don't
     *                                       report that they support the type? This exists because some remotes <strong>do</strong>
     *                                       support {@code dense_vector} without reporting that they do. And, for a while, we used the
     *                                       query itself to opt into reading these fields.
     */
    public record FieldsInfo(
        FieldCapabilitiesResponse caps,
        @Nullable TransportVersion minTransportVersion,
        boolean currentBuildIsSnapshot,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported
    ) {
        /**
         * The {@link #minTransportVersion}, but if any remote didn't tell us the version we assume
         * that it's very, very old. This effectively disables any fields that were created "recently".
         * Which is appropriate because those fields are not supported on *almost* all versions that
         * don't return the transport version in the response.
         * <p>
         *     "Very, very old" above means that there are versions of Elasticsearch that we're wire
         *     compatible that with that don't support sending the version back. That's anything
         *     from {@code 8.19.FIRST} to {@code 9.2.0}. "Recently" means any field types we
         *     added support for after the initial release of ESQL. These fields use
         *     {@link SupportedVersion#supportedOn} rather than {@link SupportedVersion#SUPPORTED_ON_ALL_NODES}.
         *     Except for DATE_NANOS. For DATE_NANOS we got lucky/made a mistake. It wasn't widely
         *     used before ESQL added support for it and we weren't careful about enabling it. So
         *     queries on mixed version clusters that touch DATE_NANOS will fail. All the types
         *     added after that, like DENSE_VECTOR, will gracefully disable themselves when talking
         *     to older nodes.
         * </p>
         * <p>
         *     Note: Once {@link EsqlResolveFieldsResponse}'s CREATED version is live everywhere
         *     we can remove this and make sure {@link #minTransportVersion} is non-null. That'll
         *     be 10.0-ish.
         * </p>
         */
        TransportVersion effectiveMinTransportVersion() {
            return minTransportVersion != null ? minTransportVersion : TransportVersion.minimumCompatible();
        }
    }

    // public for testing only
    public static IndexResolution mergedMappings(String indexPattern, FieldsInfo fieldsInfo) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION); // too expensive to run this on a transport worker
        int numberOfIndices = fieldsInfo.caps.getIndexResponses().size();
        if (numberOfIndices == 0) {
            return IndexResolution.notFound(indexPattern);
        }

        // For each field name, store a list of the field caps responses from each index
        var collectedFieldCaps = collectFieldCaps(fieldsInfo.caps);
        Map<String, IndexFieldCapabilitiesWithSourceHash> fieldsCaps = collectedFieldCaps.fieldsCaps;
        Map<String, Integer> indexMappingHashDuplicates = collectedFieldCaps.indexMappingHashDuplicates;

        // Build hierarchical fields - it's easier to do it in sorted order so the object fields come first.
        // TODO flattened is simpler - could we get away with that?
        String[] names = fieldsCaps.keySet().toArray(new String[0]);
        Arrays.sort(names);
        Map<String, EsField> rootFields = new HashMap<>();
        Set<String> partiallyUnmappedFields = new HashSet<>();
        for (String name : names) {
            Map<String, EsField> fields = rootFields;
            String fullName = name;
            boolean isAlias = false;
            UnsupportedEsField firstUnsupportedParent = null;
            while (true) {
                int nextDot = name.indexOf('.');
                if (nextDot < 0) {
                    break;
                }
                String parent = name.substring(0, nextDot);
                EsField obj = fields.get(parent);
                if (obj == null) {
                    // Object fields can't be dimensions, so we can safely hard code that here
                    obj = new EsField(parent, OBJECT, new HashMap<>(), false, true, EsField.TimeSeriesFieldType.NONE);
                    isAlias = true;
                    fields.put(parent, obj);
                } else if (firstUnsupportedParent == null && obj instanceof UnsupportedEsField unsupportedParent) {
                    firstUnsupportedParent = unsupportedParent;
                }
                fields = obj.getProperties();
                name = name.substring(nextDot + 1);
            }
            // TODO we're careful to make isAlias match IndexResolver - but do we use it?

            var fieldCap = fieldsCaps.get(fullName);
            List<IndexFieldCapabilities> fcs = fieldCap.fieldCapabilities;
            EsField field = firstUnsupportedParent == null
                ? createField(fieldsInfo, name, fullName, fcs, isAlias)
                : new UnsupportedEsField(
                    fullName,
                    firstUnsupportedParent.getOriginalTypes(),
                    firstUnsupportedParent.getName(),
                    new HashMap<>()
                );
            fields.put(name, field);
            var isPartiallyUnmapped = fcs.size() + indexMappingHashDuplicates.getOrDefault(fieldCap.indexMappingHash, 0) < numberOfIndices;
            if (isPartiallyUnmapped) {
                partiallyUnmappedFields.add(fullName);
            }
        }

        Map<String, IndexMode> concreteIndices = Maps.newMapWithExpectedSize(fieldsInfo.caps.getIndexResponses().size());
        for (FieldCapabilitiesIndexResponse ir : fieldsInfo.caps.getIndexResponses()) {
            concreteIndices.put(ir.getIndexName(), ir.getIndexMode());
        }

        boolean allEmpty = true;
        for (FieldCapabilitiesIndexResponse ir : fieldsInfo.caps.getIndexResponses()) {
            allEmpty &= ir.get().isEmpty();
        }
        // If all the mappings are empty we return an empty set of resolved indices to line up with QL
        // Introduced with #46775
        // We need to be able to differentiate between an empty mapping index and an empty index due to fields not being found. An empty
        // mapping index will generate no columns (important) for a query like FROM empty-mapping-index, whereas an empty result here but
        // for fields that do not exist in the index (but the index has a mapping) will result in "VerificationException Unknown column"
        // errors.
        var index = new EsIndex(indexPattern, rootFields, allEmpty ? Map.of() : concreteIndices, partiallyUnmappedFields);
        var failures = EsqlCCSUtils.groupFailuresPerCluster(fieldsInfo.caps.getFailures());
        return IndexResolution.valid(index, concreteIndices.keySet(), failures);
    }

    private record IndexFieldCapabilitiesWithSourceHash(List<IndexFieldCapabilities> fieldCapabilities, String indexMappingHash) {}

    private record CollectedFieldCaps(
        Map<String, IndexFieldCapabilitiesWithSourceHash> fieldsCaps,
        // The map won't contain entries without duplicates, i.e., it's number of occurrences - 1.
        Map<String, Integer> indexMappingHashDuplicates
    ) {}

    private static CollectedFieldCaps collectFieldCaps(FieldCapabilitiesResponse fieldCapsResponse) {
        Map<String, Integer> indexMappingHashToDuplicateCount = new HashMap<>();
        Map<String, IndexFieldCapabilitiesWithSourceHash> fieldsCaps = new HashMap<>();

        for (FieldCapabilitiesIndexResponse response : fieldCapsResponse.getIndexResponses()) {
            if (indexMappingHashToDuplicateCount.compute(response.getIndexMappingHash(), (k, v) -> v == null ? 1 : v + 1) > 1) {
                continue;
            }
            for (IndexFieldCapabilities fc : response.get().values()) {
                if (fc.isMetadatafield()) {
                    // ESQL builds the metadata fields if they are asked for without using the resolution.
                    continue;
                }
                List<IndexFieldCapabilities> all = fieldsCaps.computeIfAbsent(
                    fc.name(),
                    (_key) -> new IndexFieldCapabilitiesWithSourceHash(new ArrayList<>(), response.getIndexMappingHash())
                ).fieldCapabilities;
                all.add(fc);
            }
        }

        var iterator = indexMappingHashToDuplicateCount.entrySet().iterator();
        while (iterator.hasNext()) {
            var next = iterator.next();
            if (next.getValue() <= 1) {
                iterator.remove();
            } else {
                next.setValue(next.getValue() - 1);
            }
        }

        return new CollectedFieldCaps(fieldsCaps, indexMappingHashToDuplicateCount);
    }

    private static EsField createField(
        FieldsInfo fieldsInfo,
        String name,
        String fullName,
        List<IndexFieldCapabilities> fcs,
        boolean isAlias
    ) {
        IndexFieldCapabilities first = fcs.get(0);
        List<IndexFieldCapabilities> rest = fcs.subList(1, fcs.size());
        DataType type = EsqlDataTypeRegistry.INSTANCE.fromEs(first.type(), first.metricType());
        boolean typeSupported = type.supportedVersion()
            .supportedOn(fieldsInfo.effectiveMinTransportVersion(), fieldsInfo.currentBuildIsSnapshot)
            || switch (type) {
                case AGGREGATE_METRIC_DOUBLE -> fieldsInfo.useAggregateMetricDoubleWhenNotSupported;
                case DENSE_VECTOR -> fieldsInfo.useDenseVectorWhenNotSupported;
                default -> false;
            };
        if (false == typeSupported) {
            type = UNSUPPORTED;
        }
        boolean aggregatable = first.isAggregatable();
        EsField.TimeSeriesFieldType timeSeriesFieldType = EsField.TimeSeriesFieldType.fromIndexFieldCapabilities(first);
        if (rest.isEmpty() == false) {
            for (IndexFieldCapabilities fc : rest) {
                if (first.metricType() != fc.metricType()) {
                    return conflictingMetricTypes(name, fullName, fieldsInfo.caps);
                }
                try {
                    timeSeriesFieldType = timeSeriesFieldType.merge(EsField.TimeSeriesFieldType.fromIndexFieldCapabilities(fc));
                } catch (IllegalArgumentException e) {
                    return new InvalidMappedField(name, e.getMessage());
                }
            }
            for (IndexFieldCapabilities fc : rest) {
                if (type != EsqlDataTypeRegistry.INSTANCE.fromEs(fc.type(), fc.metricType())) {
                    return conflictingTypes(name, fullName, fieldsInfo.caps);
                }
            }
            for (IndexFieldCapabilities fc : rest) {
                aggregatable &= fc.isAggregatable();
            }
        }

        // TODO I think we only care about unmapped fields if we're aggregating on them. do we even then?

        if (type == TEXT) {
            return new TextEsField(name, new HashMap<>(), false, isAlias, timeSeriesFieldType);
        }
        if (type == KEYWORD) {
            int length = Short.MAX_VALUE;
            // TODO: to check whether isSearchable/isAggregateable takes into account the presence of the normalizer
            boolean normalized = false;
            return new KeywordEsField(name, new HashMap<>(), aggregatable, length, normalized, isAlias, timeSeriesFieldType);
        }
        if (type == DATETIME) {
            return DateEsField.dateEsField(name, new HashMap<>(), aggregatable, timeSeriesFieldType);
        }
        if (type == UNSUPPORTED) {
            return unsupported(name, first);
        }

        return new EsField(name, type, new HashMap<>(), aggregatable, isAlias, timeSeriesFieldType);
    }

    private static UnsupportedEsField unsupported(String name, IndexFieldCapabilities fc) {
        String originalType = fc.metricType() == TimeSeriesParams.MetricType.COUNTER ? "counter" : fc.type();
        return new UnsupportedEsField(name, List.of(originalType));
    }

    private static EsField conflictingTypes(String name, String fullName, FieldCapabilitiesResponse fieldCapsResponse) {
        Map<String, Set<String>> typesToIndices = new TreeMap<>();
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            IndexFieldCapabilities fc = ir.get().get(fullName);
            if (fc != null) {
                DataType type = EsqlDataTypeRegistry.INSTANCE.fromEs(fc.type(), fc.metricType());
                if (type == UNSUPPORTED) {
                    return unsupported(name, fc);
                }
                typesToIndices.computeIfAbsent(type.typeName(), _key -> new TreeSet<>()).add(ir.getIndexName());
            }
        }
        return new InvalidMappedField(name, typesToIndices);
    }

    private static EsField conflictingMetricTypes(String name, String fullName, FieldCapabilitiesResponse fieldCapsResponse) {
        TreeSet<String> indices = new TreeSet<>();
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            IndexFieldCapabilities fc = ir.get().get(fullName);
            if (fc != null) {
                indices.add(ir.getIndexName());
            }
        }
        return new InvalidMappedField(name, "mapped as different metric types in indices: " + indices);
    }

    private static FieldCapabilitiesRequest createFieldCapsRequest(
        String index,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions
    ) {
        FieldCapabilitiesRequest req = new FieldCapabilitiesRequest().indices(Strings.commaDelimitedListToStringArray(index));
        req.fields(fieldNames.toArray(String[]::new));
        req.includeUnmapped(true);
        req.indexFilter(requestFilter);
        req.returnLocalAll(false);
        // lenient because we throw our own errors looking at the response e.g. if something was not resolved
        // also because this way security doesn't throw authorization exceptions but rather honors ignore_unavailable
        req.indicesOptions(FIELD_CAPS_INDICES_OPTIONS);
        // we ignore the nested data type fields starting with https://github.com/elastic/elasticsearch/pull/111495
        if (includeAllDimensions) {
            req.filters("-nested", "+dimension");
        } else {
            req.filters("-nested");
        }
        req.setMergeResults(false);
        return req;
    }
}
