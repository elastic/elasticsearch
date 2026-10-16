/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.CrossProjectModeOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsRequest;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.action.PlanningCpuTracker;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.CompactInvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedTsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;
import org.elasticsearch.xpack.esql.core.type.SupportedVersion;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.esql.view.IndexViewOrigins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLATTENED;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;

public class IndexResolver {

    private static final Logger LOGGER = LogManager.getLogger(IndexResolver.class);

    public static final Set<String> ALL_FIELDS = Set.of("*");
    public static final Set<String> INDEX_METADATA_FIELD = Set.of(MetadataAttribute.INDEX);

    public static final IndicesOptions DEFAULT_OPTIONS = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            IndicesOptions.WildcardOptions.builder().matchOpen(true).matchClosed(false).includeHidden(false).allowEmptyExpressions(true)
        )
        .gatekeeperOptions(
            IndicesOptions.GatekeeperOptions.builder().ignoreThrottled(true).allowClosedIndices(true).allowAliasToMultipleIndices(true)
        )
        .build();

    /**
     * Configuration options used for resolving indices in a CPS context.
     * Those options shift index resolution validation to FieldCaps action itself
     * as well as automatically expand flat expressions to multiple qualified ones.
     */
    private static final IndicesOptions FLAT_STRICT_OPTIONS = IndicesOptions.builder(DEFAULT_OPTIONS)
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .crossProjectModeOptions(new CrossProjectModeOptions(true))
        .build();

    /**
     * Same as above, except lenient (eg allows expressions to be missing)
     */
    private static final IndicesOptions FLAT_LENIENT_OPTIONS = IndicesOptions.builder(DEFAULT_OPTIONS)
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .crossProjectModeOptions(new CrossProjectModeOptions(true))
        .build();

    private final Client client;
    /**
     * Whether the {@code flattened} data type is currently enabled, backing the {@code esql.query.flattened.enabled}
     * kill switch. Read at field-caps resolution time so a runtime change takes effect on the next query. Tests that
     * don't exercise the kill switch pass an always-enabled supplier ({@code () -> true}).
     */
    private final BooleanSupplier flattenedDataTypeEnabled;

    public IndexResolver(Client client, BooleanSupplier flattenedDataTypeEnabled) {
        this.client = client;
        this.flattenedDataTypeEnabled = flattenedDataTypeEnabled;
    }

    /**
     * Whether the {@code flattened} data type is enabled right now.
     */
    public boolean flattenedDataTypeEnabled() {
        return flattenedDataTypeEnabled.getAsBoolean();
    }

    /**
     * Like {@code IndexResolver#resolveIndicesVersioned}
     * but simplified and does not pass on the determined minimum transport version to the listener.
     */
    public void resolveLookupIndices(
        String indexPattern,
        Set<String> fieldNames,
        TransportVersion minimumVersion,
        ActionListener<IndexResolution> listener
    ) {
        doResolveIndices(
            createResolveFieldRequest(DEFAULT_OPTIONS, indexPattern, null, fieldNames, null, false, false),
            indexPattern,
            false, /* lookup indices should do not be empty */
            minimumVersion,
            false,
            false,
            false,
            false, /* HIGHLIGHT only reads the analyzer groups of the main indices */
            false,
            null,
            null,
            DO_NOT_GROUP,
            listener.map(Versioned::inner)
        );
    }

    /**
     * Perform a field caps request to resolve a pattern to one mapping (potentially compound, meaning it spans multiple indices).
     * <p>
     * The field caps response contains the minimum transport version of all clusters that apply to the pattern,
     * and it is used to deal with previously unsupported data types during resolution.
     * <p>
     * If a field's type is not supported on the minimum version, it will be {@link DataType#UNSUPPORTED}.
     * <p>
     * If the nodes are too old to include their minimum transport version in the field caps response, we'll assume
     * {@link TransportVersion#minimumCompatible()}.
     * <p>
     * The {@code minimumVersion} already known so far must be passed in and will be used instead of the minimum version
     * from the field caps response if it is lower. During main index resolution, this is the local cluster's minimum version.
     * This safeguards against using too new a version in case of {@code FROM remote_only:* | ...} queries that don't have any indices
     * on the local cluster.
     * <p>
     * But it's also important for remote {@code ENRICH} resolution, because in CCS enrich policies are resolved on remote clusters,
     * so the overall minimum transport version that the coordinating cluster observed must be passed in here to avoid inconsistencies.
     * <p>
     * The overall minimum version is updated using the field caps response and is passed on to the listener.
     */
    public void resolveMainIndicesVersioned(
        String indexPattern,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        TransportVersion minimumVersion,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        boolean needsAnalyzerGroups,
        boolean trackUnmappedFieldIndices,
        @Nullable Set<String> flattenedKeys,
        IndicesExpressionGrouper indicesExpressionGrouper,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        doResolveIndices(
            createResolveFieldRequest(DEFAULT_OPTIONS, indexPattern, null, fieldNames, requestFilter, includeAllDimensions, false),
            indexPattern,
            true, /* allow empty index resolution when resolving main pattern */
            minimumVersion,
            useAggregateMetricDoubleWhenNotSupported,
            useDenseVectorWhenNotSupported,
            hasTimeSeriesAggregation,
            needsAnalyzerGroups,
            trackUnmappedFieldIndices,
            flattenedKeys,
            null,
            (indexPattern1, fieldCapabilitiesResponse) -> Maps.transformValues(
                indicesExpressionGrouper.groupIndices(IndicesOptions.DEFAULT, Strings.splitStringByCommaToArray(indexPattern1), false),
                v -> List.of(v.indices())
            ),
            listener
        );
    }

    /**
     * Resolves the exemplar data streams of a query run with {@code SET exemplars=true} to all of their fields. Their pattern is
     * derived from the resolved time series indices (see {@link ExemplarsRewriter#exemplarIndexPattern}), so unlike a
     * user-written pattern some of them may not exist: field caps tolerates that, but executing the query against them would not. So
     * field caps is asked which indices it resolved each expression to, and only the expressions that resolved to something become the
     * original indices of the relation (see {@link #RESOLVED_EXPRESSIONS}). Field caps only reports that when every cluster involved
     * supports it; otherwise the resolution fails.
     * <p>
     * The relation is a regular one, not a time series one, so none of the time series or unmapped field handling of
     * {@link #resolveMainIndicesVersioned} applies; {@code requestFilter} and {@code minimumVersion} work like there.
     */
    public void resolveExemplarDataStreamsVersioned(
        String indexPattern,
        QueryBuilder requestFilter,
        TransportVersion minimumVersion,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        doResolveIndices(
            createResolveFieldRequest(DEFAULT_OPTIONS, indexPattern, null, ALL_FIELDS, requestFilter, false, true),
            indexPattern,
            true, /* the derived data streams may not exist */
            minimumVersion,
            false,
            false,
            false,
            false,
            false,
            null,
            null,
            RESOLVED_EXPRESSIONS,
            listener
        );
    }

    /**
     * Original indices for a pattern whose expressions may not all exist: per cluster, the requested expressions that field caps
     * resolved to at least one index. Requires the field caps response to carry that resolution information, which the coordinating
     * node only includes when it was requested and all clusters involved are recent enough to provide it.
     */
    static final OriginalIndexExtractor RESOLVED_EXPRESSIONS = (indexPattern, fieldCapabilitiesResponse) -> {
        if (fieldCapabilitiesResponse.getResolvedLocally() == null) {
            // the coordinating node leaves the resolution information out unless all nodes of all clusters involved support the
            // transport version [resolved_fields_caps], which was introduced in 9.3.0
            throw new VerificationException(
                "When querying exemplars, all nodes in all involved clusters must be at least at version 9.3.0; "
                    + "cannot resolve which of [{}] exist otherwise",
                indexPattern
            );
        }
        return Maps.transformValues(EsqlResolvedIndexExpression.from(fieldCapabilitiesResponse), v -> List.copyOf(v.expression()));
    };

    /**
     * Like {@code IndexResolver#resolveIndicesVersioned} but for flat (CPS) queries. Set
     * {@code lenient} to allow targets to be missing — used for {@code ViewShadowRelation}
     * lookups, where finding nothing is the expected outcome when no remote project has an
     * index matching the local view name. {@code lenient=false} is the default for the
     * strict main index resolution path.
     */
    public void resolveFlatIndicesVersioned(
        boolean lenient,
        String indexPattern,
        String projectRouting,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        TransportVersion minimumVersion,
        // Used for bwc with 9.2.0, which supports aggregate_metric_double but doesn't provide its version in the field
        // caps response. We'll just assume the type is supported based on usage in the query to not break compatibility
        // with 9.2.0.
        boolean useAggregateMetricDoubleWhenNotSupported,
        // Same as above
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        boolean needsAnalyzerGroups,
        boolean trackUnmappedFieldIndices,
        @Nullable Set<String> flattenedKeys,
        @Nullable Consumer<TargetProjects> routingInfoCapture,
        IndexViewOrigins indexViewOrigins,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        IndicesOptions options = lenient ? FLAT_LENIENT_OPTIONS : FLAT_STRICT_OPTIONS;
        doResolveIndices(
            createResolveFieldRequest(options, indexPattern, projectRouting, fieldNames, requestFilter, includeAllDimensions, true),
            indexPattern,
            true, /* flat index expression could resolve to empty */
            minimumVersion,
            useAggregateMetricDoubleWhenNotSupported,
            useDenseVectorWhenNotSupported,
            hasTimeSeriesAggregation,
            needsAnalyzerGroups,
            trackUnmappedFieldIndices,
            flattenedKeys,
            routingInfoCapture,
            (innerIndexPattern, fieldCapabilitiesResponse) -> Maps.transformValues(
                EsqlResolvedIndexExpression.from(fieldCapabilitiesResponse),
                v -> List.copyOf(v.expression())
            ),
            listener.delegateResponse((l, e) -> {
                if (e instanceof IllegalArgumentException
                    && e.getMessage().startsWith(RemoteClusterAware.ALL_INDICES_EXCLUDED_MESSAGE_PREFIX)) {
                    l.onResponse(new Versioned<>(IndexResolution.empty(indexPattern), TransportVersion.minimumCompatible()));
                    return;
                }
                var infe = (IndexNotFoundException) ExceptionsHelper.unwrap(e, IndexNotFoundException.class);
                if (infe != null) {
                    l.onFailure(new VerificationException(EsqlCCSUtils.unknownIndexError(infe.getIndex().getName(), indexViewOrigins)));
                    return;
                }
                l.onFailure(e);
            })
        );
    }

    private void doResolveIndices(
        EsqlResolveFieldsRequest request,
        String indexPattern,
        boolean allowEmpty,
        TransportVersion minimumVersion,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        boolean needsAnalyzerGroups,
        boolean trackUnmappedFieldIndices,
        @Nullable Set<String> flattenedKeys,
        @Nullable Consumer<TargetProjects> routingInfoCapture,
        OriginalIndexExtractor originalIndexExtractor,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        ActionListener<EsqlResolveFieldsResponse> responseHandler = listener.delegateFailureAndWrap((l, response) -> {
            if (routingInfoCapture != null) {
                TargetProjects tp = request.getResolvedTargetProjects();
                if (tp != null) {
                    routingInfoCapture.accept(tp);
                }
            }
            TransportVersion responseMinimumVersion = response.caps().minTransportVersion();
            // Note: Once {@link EsqlResolveFieldsResponse}'s CREATED version is live everywhere
            // we can remove this and make sure responseMinimumVersion is non-null. That'll be 10.0-ish.
            TransportVersion overallMinimumVersion = responseMinimumVersion == null
                ? TransportVersion.minimumCompatible()
                : TransportVersion.min(minimumVersion, responseMinimumVersion);

            FieldsInfo info = new FieldsInfo(
                response.caps(),
                overallMinimumVersion,
                Build.current().isSnapshot(),
                useAggregateMetricDoubleWhenNotSupported,
                useDenseVectorWhenNotSupported,
                hasTimeSeriesAggregation,
                needsAnalyzerGroups,
                flattenedDataTypeEnabled.getAsBoolean(),
                flattenedKeys
            );
            LOGGER.debug(
                "previously assumed minimum transport version [{}] updated to effective version [{}]"
                    + " using field caps response version [{}] for index pattern [{}]",
                minimumVersion,
                info.minTransportVersion(),
                responseMinimumVersion,
                indexPattern
            );

            l.onResponse(
                new Versioned<>(
                    mergedMappings(indexPattern, allowEmpty, info, trackUnmappedFieldIndices, originalIndexExtractor),
                    info.minTransportVersion()
                )
            );
        });
        // Meter the response handling (mergedMappings can take tens of ms for wide mappings) as planning CPU of the query
        // that dispatched this request, if one is metering this thread.
        client.execute(EsqlResolveFieldsAction.TYPE, request, PlanningCpuTracker.inheritMeteredCpu(responseHandler));
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
     *                            If any remote didn't tell us the version we assume
     *                            that it's very, very old. This effectively disables any fields that were created "recently".
     *                            Which is appropriate because those fields are not supported on *almost* all versions that
     *                            don't return the transport version in the response.
     *                            <p>
     *                            "Very, very old" above means that there are versions of Elasticsearch that we're wire
     *                            compatible that with that don't support sending the version back. That's anything
     *                            from {@code 8.19.FIRST} to {@code 9.2.0}. "Recently" means any field types we
     *                            added support for after the initial release of ESQL. These fields use
     *                            {@link SupportedVersion#supportedOn} rather than {@link SupportedVersion#SUPPORTED_ON_ALL_NODES}.
     *                            Except for DATE_NANOS. For DATE_NANOS we got lucky/made a mistake. It wasn't widely
     *                            used before ESQL added support for it and we weren't careful about enabling it. So
     *                            queries on mixed version clusters that touch DATE_NANOS will fail. All the types
     *                            added after that, like DENSE_VECTOR, will gracefully disable themselves when talking
     *                            to older nodes.
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
     * @param hasTimeSeriesAggregation whether the query contains a time series aggregation (a {@code TS} source followed by
     *                                {@code STATS}). When {@code false}, time series field type consistency checks
     *                                (dimension vs metric conflicts across indices) are skipped, avoiding spurious
     *                                {@link InvalidMappedField} errors for queries that don't aggregate time series data.
     * @param needsAnalyzerGroups whether the query has a HIGHLIGHT that uses the mapping analyzers. When {@code false}, a text
     *                            field whose indices disagree on the analyzer does not record which index uses which, since
     *                            only HIGHLIGHT reads it and the index names can be many.
     * @param flattenedDataTypeEnabled whether the {@code flattened} data type is enabled (the {@code esql.query.flattened.enabled}
     *                                 kill switch). When {@code false}, {@code flattened} fields are resolved as
     *                                 {@link DataType#UNSUPPORTED}, reverting to pre-flattened-support behavior.
     * @param flattenedKeys the names the query uses, each a {@code keyword}, the key of that name, in the indices where it is under
     *                      a {@code flattened} field; {@code null} if names under a {@code flattened} field are not its keys (see
     *                      {@link Configuration#flattenedKeys()})
     */
    public record FieldsInfo(
        FieldCapabilitiesResponse caps,
        @Nullable TransportVersion minTransportVersion,
        boolean currentBuildIsSnapshot,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        boolean needsAnalyzerGroups,
        boolean flattenedDataTypeEnabled,
        @Nullable Set<String> flattenedKeys
    ) {}

    // public for testing only
    public static IndexResolution mergedMappings(
        String indexPattern,
        boolean allowEmpty,
        FieldsInfo fieldsInfo,
        boolean trackUnmappedFieldIndices,
        OriginalIndexExtractor originalIndexExtractor
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION); // too expensive to run this on a transport worker
        List<FieldCapabilitiesIndexResponse> indexResponses = fieldsInfo.caps.getIndexResponses();
        int numberOfIndices = indexResponses.size();
        if (numberOfIndices == 0) {
            return allowEmpty ? IndexResolution.empty(indexPattern) : IndexResolution.notFound(indexPattern);
        }
        List<IndexFields> indexFields = indexFields(indexResponses, flattenedKeys(fieldsInfo) ? fieldsInfo.flattenedKeys() : null);

        // For each field name, store a list of the field caps responses from each index
        var collectedFieldCaps = collectFieldCaps(indexFields, trackUnmappedFieldIndices);
        Map<String, IndexFieldCapabilitiesWithSourceHash> fieldsCaps = collectedFieldCaps.fieldsCaps;

        // Build hierarchical fields - it's easier to do it in sorted order so the object fields come first.
        // TODO flattened is simpler - could we get away with that?
        String[] names = fieldsCaps.keySet().toArray(new String[0]);
        Arrays.sort(names);
        Map<String, EsField> rootFields = new HashMap<>();
        Map<Set<String>, Set<String>> indexDedupCache = useCompactInvalidMappedField(fieldsInfo.minTransportVersion())
            ? new HashMap<>()
            : null;
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
                ? createField(fieldsInfo, indexFields, name, fullName, fcs, isAlias, indexDedupCache)
                : new UnsupportedEsField(
                    fullName,
                    firstUnsupportedParent.getOriginalTypes(),
                    firstUnsupportedParent.getName(),
                    new HashMap<>()
                );
            if (trackUnmappedFieldIndices) {
                Set<String> mappedIndices = collectedFieldCaps.fieldToMappedIndices.getOrDefault(fullName, Set.of());
                field = wrapIfPartiallyUnmapped(field, name, fullName, mappedIndices, numberOfIndices, indexDedupCache);
            }
            fields.put(name, field);
        }

        boolean allEmpty = true;
        Map<String, IndexProperties> indexProperties = Maps.newMapWithExpectedSize(indexResponses.size());
        Map<String, List<String>> concreteIndices = Maps.newHashMapWithExpectedSize(8);
        for (FieldCapabilitiesIndexResponse ir : indexResponses) {
            allEmpty &= ir.get().isEmpty();
            indexProperties.put(ir.getIndexName(), new IndexProperties(ir.getIndexMode(), ir.getNumberOfShards()));
            var split = RemoteClusterAware.splitIndexName(ir.getIndexName());
            concreteIndices.computeIfAbsent(split.getClusterGroupingKey(), k -> new ArrayList<>()).add(split.indexExpression());
        }

        // If all the mappings are empty we return an empty set of resolved indices to line up with QL
        // Introduced with #46775
        // We need to be able to differentiate between an empty mapping index and an empty index due to fields not being found. An empty
        // mapping index will generate no columns (important) for a query like FROM empty-mapping-index, whereas an empty result here but
        // for fields that do not exist in the index (but the index has a mapping) will result in "VerificationException Unknown column"
        // errors.
        var resolvedProperties = allEmpty ? Map.<String, IndexProperties>of() : indexProperties;
        var index = new EsIndex(
            indexPattern,
            rootFields,
            resolvedProperties,
            // instead of using indexSplitter we could use original indices from
            // FieldCapabilitiesResponse#resolvedLocally and FieldCapabilitiesResponse#resolvedRemotely
            // once all remotes support it (v9.3+)
            originalIndexExtractor.apply(indexPattern, fieldsInfo.caps),
            concreteIndices
        );
        var failures = EsqlCCSUtils.groupFailuresPerCluster(fieldsInfo.caps.getFailures());
        return IndexResolution.valid(index, indexProperties.keySet(), failures);
    }

    private record IndexFieldCapabilitiesWithSourceHash(List<IndexFieldCapabilities> fieldCapabilities, String indexMappingHash) {}

    private record CollectedFieldCaps(
        Map<String, IndexFieldCapabilitiesWithSourceHash> fieldsCaps,
        // The map won't contain entries without duplicates, i.e., it's number of occurrences - 1.
        Map<String, Integer> indexMappingHashDuplicates,
        Map<String, Set<String>> fieldToMappedIndices
    ) {}

    /**
     * The fields of an index: those field caps reports and, with {@link FieldsInfo#flattenedKeys()}, a {@code keyword} for each
     * name under one of its {@code flattened} fields that the query uses or another index maps.
     */
    private record IndexFields(FieldCapabilitiesIndexResponse response, Map<String, IndexFieldCapabilities> fields) {}

    private static List<IndexFields> indexFields(List<FieldCapabilitiesIndexResponse> responses, @Nullable Set<String> namedFields) {
        List<IndexFields> indexFields = new ArrayList<>(responses.size());
        Set<String> values = namedFields == null ? Set.of() : valueNames(responses);
        Set<String> leaves = null;
        // Indices with the same mapping share their fields
        Map<Map<String, IndexFieldCapabilities>, Map<String, IndexFieldCapabilities>> resolved = new IdentityHashMap<>();
        for (FieldCapabilitiesIndexResponse response : responses) {
            Map<String, IndexFieldCapabilities> fields = response.get();
            Map<String, IndexFieldCapabilities> keys = resolved.get(fields);
            if (keys == null && namedFields != null) {
                boolean hasFlattened = false;
                for (IndexFieldCapabilities fc : fields.values()) {
                    // A name is mapped only where it is a leaf: where another index maps it as a field that holds values, an
                    // object does not
                    if (ObjectMapper.CONTENT_TYPE.equals(fc.type()) && values.contains(fc.name())) {
                        if (keys == null) {
                            keys = new HashMap<>(fields);
                        }
                        keys.remove(fc.name());
                    }
                    hasFlattened |= FlattenedFieldMapper.CONTENT_TYPE.equals(fc.type());
                }
                if (hasFlattened) {
                    if (leaves == null) {
                        leaves = leafNames(responses, namedFields);
                    }
                    for (String name : leaves) {
                        if (fields.containsKey(name)) {
                            continue;
                        }
                        IndexFieldCapabilities root = flattenedRoot(fields, name);
                        if (root != null) {
                            if (keys == null) {
                                keys = new HashMap<>(fields);
                            }
                            keys.put(name, flattenedKey(name, root));
                        }
                    }
                }
                resolved.put(fields, keys == null ? fields : keys);
            }
            indexFields.add(new IndexFields(response, keys == null ? fields : keys));
        }
        return indexFields;
    }

    /** The names some index maps as a field that holds values, rather than as an object of other fields. */
    private static Set<String> valueNames(List<FieldCapabilitiesIndexResponse> responses) {
        Set<String> names = new HashSet<>();
        for (FieldCapabilitiesIndexResponse response : responses) {
            for (IndexFieldCapabilities fc : response.get().values()) {
                if (fc.isMetadatafield() == false && isObject(fc) == false) {
                    names.add(fc.name());
                }
            }
        }
        return names;
    }

    private static boolean isObject(IndexFieldCapabilities fc) {
        return ObjectMapper.CONTENT_TYPE.equals(fc.type()) || NestedObjectMapper.CONTENT_TYPE.equals(fc.type());
    }

    /**
     * The names the query uses and the names some index maps as a field that holds values, rather than as an object of other
     * fields: the names a {@code flattened} field of another index holds as keys.
     */
    private static Set<String> leafNames(List<FieldCapabilitiesIndexResponse> responses, Set<String> namedFields) {
        Set<String> leaves = new HashSet<>(namedFields);
        for (FieldCapabilitiesIndexResponse response : responses) {
            for (IndexFieldCapabilities fc : response.get().values()) {
                if (fc.isMetadatafield() == false && fc.name().indexOf('.') > 0 && isObject(fc) == false) {
                    leaves.add(fc.name());
                }
            }
        }
        return leaves;
    }

    /** The {@code flattened} field of {@code fields} that {@code name} is under, if any. */
    @Nullable
    private static IndexFieldCapabilities flattenedRoot(Map<String, IndexFieldCapabilities> fields, String name) {
        for (int dot = name.indexOf('.'); dot > 0; dot = name.indexOf('.', dot + 1)) {
            IndexFieldCapabilities parent = fields.get(name.substring(0, dot));
            if (parent != null && FlattenedFieldMapper.CONTENT_TYPE.equals(parent.type())) {
                return parent;
            }
        }
        return null;
    }

    /** The {@code keyword} that {@code name} is in an index where it is under the {@code flattened} field {@code root}. */
    private static IndexFieldCapabilities flattenedKey(String name, IndexFieldCapabilities root) {
        return new IndexFieldCapabilities(
            name,
            KeywordFieldMapper.CONTENT_TYPE,
            false,
            root.isSearchable(),
            root.isAggregatable(),
            false,
            false,
            null,
            null,
            Map.of(),
            null,
            TextFieldMapper.Defaults.POSITION_INCREMENT_GAP,
            false
        );
    }

    private static CollectedFieldCaps collectFieldCaps(List<IndexFields> indexFields, boolean trackUnmappedFieldIndices) {
        Map<String, Integer> indexMappingHashToDuplicateCount = new HashMap<>();
        Map<String, IndexFieldCapabilitiesWithSourceHash> fieldsCaps = new HashMap<>();
        Map<String, Set<String>> fieldToMappedIndices = trackUnmappedFieldIndices ? new HashMap<>() : null;

        for (IndexFields index : indexFields) {
            FieldCapabilitiesIndexResponse response = index.response();
            boolean isNew = indexMappingHashToDuplicateCount.compute(response.getIndexMappingHash(), (k, v) -> v == null ? 1 : v + 1) <= 1;
            String indexName = response.getIndexName();
            for (IndexFieldCapabilities fc : index.fields().values()) {
                if (fc.isMetadatafield()) {
                    // ESQL builds the metadata fields if they are asked for without using the resolution.
                    continue;
                }
                if (isNew) {
                    List<IndexFieldCapabilities> all = fieldsCaps.computeIfAbsent(
                        fc.name(),
                        (_key) -> new IndexFieldCapabilitiesWithSourceHash(new ArrayList<>(), response.getIndexMappingHash())
                    ).fieldCapabilities;
                    all.add(fc);
                }
                if (trackUnmappedFieldIndices) {
                    fieldToMappedIndices.computeIfAbsent(fc.name(), k -> new HashSet<>()).add(indexName);
                }
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

        return new CollectedFieldCaps(fieldsCaps, indexMappingHashToDuplicateCount, fieldToMappedIndices);
    }

    /**
     * Decides whether {@code type} can be used as-is, or whether the field must be downgraded to
     * {@link DataType#UNSUPPORTED}. Besides the usual transport-version/feature gating, this enforces the
     * {@code esql.query.flattened.enabled} kill switch: when that switch is off, {@code flattened} fields are reported as
     * unsupported, reverting to the pre-flattened-support behavior (a {@code FROM} still returns the column, but as
     * unsupported, and any explicit use of it errors).
     */
    private static boolean isTypeSupported(DataType type, FieldsInfo fieldsInfo) {
        if (type == FLATTENED && fieldsInfo.flattenedDataTypeEnabled() == false) {
            return false;
        }
        if (type.supportedVersion().supportedOn(fieldsInfo.minTransportVersion(), fieldsInfo.currentBuildIsSnapshot())) {
            return true;
        }
        return switch (type) {
            case AGGREGATE_METRIC_DOUBLE -> fieldsInfo.useAggregateMetricDoubleWhenNotSupported();
            case DENSE_VECTOR -> fieldsInfo.useDenseVectorWhenNotSupported();
            default -> false;
        };
    }

    /**
     * Whether a name under a {@code flattened} field is its key: nodes before the keys of flattened fields were fields would not
     * read them.
     */
    private static boolean flattenedKeys(FieldsInfo fieldsInfo) {
        return fieldsInfo.flattenedKeys() != null
            && fieldsInfo.minTransportVersion() != null
            && fieldsInfo.minTransportVersion().supports(Configuration.ESQL_SHARD_RESOLVED_FIELDS);
    }

    private static EsField createField(
        FieldsInfo fieldsInfo,
        List<IndexFields> indexFields,
        String name,
        String fullName,
        List<IndexFieldCapabilities> fcs,
        boolean isAlias,
        // If this isn't null, it also means we need to create CompactInvalidMappedField instead of InvalidMappedField.
        @Nullable Map<Set<String>, Set<String>> indexDedupCache
    ) {
        IndexFieldCapabilities first = fcs.get(0);
        List<IndexFieldCapabilities> rest = fcs.subList(1, fcs.size());
        DataType type = EsqlDataTypeRegistry.INSTANCE.fromEs(first.type(), first.metricType());
        if (isTypeSupported(type, fieldsInfo) == false) {
            type = UNSUPPORTED;
        }
        boolean aggregatable = first.isAggregatable();
        boolean isPassthrough = Boolean.TRUE.equals(first.isPassthrough());
        EsField.TimeSeriesFieldType timeSeriesFieldType = fieldsInfo.hasTimeSeriesAggregation()
            ? EsField.TimeSeriesFieldType.fromIndexFieldCapabilities(first)
            : EsField.TimeSeriesFieldType.NONE;
        if (rest.isEmpty() == false) {
            if (fieldsInfo.hasTimeSeriesAggregation()) {
                for (IndexFieldCapabilities fc : rest) {
                    EsField.TimeSeriesFieldType next = EsField.TimeSeriesFieldType.fromIndexFieldCapabilities(fc);
                    try {
                        timeSeriesFieldType = timeSeriesFieldType.merge(next);
                    } catch (IllegalArgumentException e) {
                        // Surface time series metadata conflicts (dimension vs metric) as an InvalidMappedTsField.
                        // Don't use UnsupportedEsField as subfields would also be marked as unsupported; also don't use InvalidMappedField
                        // as this is for type conflicts that can be resolved by casting.
                        return new InvalidMappedTsField(name, e.getMessage());
                    }
                }
            }
            for (IndexFieldCapabilities fc : rest) {
                if (type != EsqlDataTypeRegistry.INSTANCE.fromEs(fc.type(), fc.metricType())) {
                    return conflictingTypes(name, fullName, indexFields, indexDedupCache);
                }
            }
            for (IndexFieldCapabilities fc : rest) {
                aggregatable &= fc.isAggregatable();
                isPassthrough &= Boolean.TRUE.equals(fc.isPassthrough());
            }
        }

        // TODO I think we only care about unmapped fields if we're aggregating on them. do we even then?

        if (type == TEXT) {
            return textField(name, fullName, isAlias, timeSeriesFieldType, fcs, fieldsInfo);
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

        return new EsField(name, type, new HashMap<>(), aggregatable, isAlias, timeSeriesFieldType, isPassthrough);
    }

    /**
     * Keeps the index analyzer when every index agrees on the name and gap. A mix records which indices use which
     * analyzer as {@link TextEsField#analyzerGroups()} when {@link FieldsInfo#needsAnalyzerGroups()}.
     */
    private static TextEsField textField(
        String name,
        String fullName,
        boolean isAlias,
        EsField.TimeSeriesFieldType timeSeriesFieldType,
        List<IndexFieldCapabilities> fcs,
        FieldsInfo fieldsInfo
    ) {
        String analyzer = fcs.getFirst().indexAnalyzer();
        int gap = fcs.getFirst().indexAnalyzerPositionIncrementGap();
        boolean shared = analyzer != null
            && fcs.stream().allMatch(fc -> analyzer.equals(fc.indexAnalyzer()) && gap == fc.indexAnalyzerPositionIncrementGap());
        TextEsField.UnknownAnalyzer unknown;
        List<IndexAnalyzerGroup> groups = null;
        if (shared) {
            unknown = TextEsField.UnknownAnalyzer.NONE;
        } else if (analyzer != null || fcs.stream().anyMatch(fc -> fc.indexAnalyzer() != null)) {
            // A non-null first analyzer that is not shared is already a conflict; otherwise look for any named peer.
            unknown = TextEsField.UnknownAnalyzer.CONFLICT;
            groups = fieldsInfo.needsAnalyzerGroups() ? analyzerGroups(fullName, fieldsInfo.caps()) : null;
        } else if (fcs.stream().anyMatch(IndexFieldCapabilities::indexLocalAnalyzer)) {
            unknown = TextEsField.UnknownAnalyzer.INDEX_LOCAL;
        } else {
            unknown = TextEsField.UnknownAnalyzer.NOT_REPORTED;
        }
        return new TextEsField(name, new HashMap<>(), false, isAlias, timeSeriesFieldType, shared ? analyzer : null, gap, unknown, groups);
    }

    /** Like {@link #conflictingTypes}, walks every index response since {@code fcs} is deduplicated by mapping hash. */
    private static List<IndexAnalyzerGroup> analyzerGroups(String fullName, FieldCapabilitiesResponse fieldCapsResponse) {
        Map<String, IndexAnalyzerGroup.Analyzer> analyzerByIndex = new LinkedHashMap<>();
        for (FieldCapabilitiesIndexResponse ir : fieldCapsResponse.getIndexResponses()) {
            IndexFieldCapabilities fc = ir.get().get(fullName);
            if (fc != null) {
                // IndexFieldCapabilities already normalizes the gap to the default when the name is null.
                analyzerByIndex.put(
                    ir.getIndexName(),
                    new IndexAnalyzerGroup.Analyzer(fc.indexAnalyzer(), fc.indexLocalAnalyzer(), fc.indexAnalyzerPositionIncrementGap())
                );
            }
        }
        return IndexAnalyzerGroup.byAnalyzer(analyzerByIndex);
    }

    // Visible for testing.
    public static EsField wrapIfPartiallyUnmapped(
        EsField field,
        String name,
        String fullName,
        Set<String> mappedIndices,
        int numberOfIndices
    ) {
        return wrapIfPartiallyUnmapped(field, name, fullName, mappedIndices, numberOfIndices, null);
    }

    public static EsField wrapIfPartiallyUnmapped(
        EsField field,
        String name,
        String fullName,
        Set<String> mappedIndices,
        int numberOfIndices,
        // If this isn't null, it also means we need to create CompactInvalidMappedField instead of InvalidMappedField.
        @Nullable Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return field instanceof UnsupportedEsField == false && mappedIndices.size() < numberOfIndices
            ? wrapPartiallyUnmappedField(field, name, fullName, mappedIndices, indexDedupCache)
            : field;
    }

    // Visible for testing
    public static EsField wrapPartiallyUnmappedField(EsField field, String name, String fullName, Set<String> mappedIndices) {
        return wrapPartiallyUnmappedField(field, name, fullName, mappedIndices, null);
    }

    public static EsField wrapPartiallyUnmappedField(
        EsField field,
        String name,
        String fullName,
        Set<String> mappedIndices,
        // If this isn't null, it also means we need to create CompactInvalidMappedField instead of InvalidMappedField.
        @Nullable Map<Set<String>, Set<String>> indexDedupCache
    ) {
        boolean useLegacyField = indexDedupCache == null;
        return switch (field.getDataType()) {
            // OBJECT fields are containers for subfields, not leaf fields that get queried directly.
            // Wrapping them would break downstream code that doesn't expect OBJECT as a data type in InvalidMappedField.
            case OBJECT -> field;
            case KEYWORD -> new PotentiallyUnmappedKeywordEsField(name);
            default -> {
                if (field instanceof TypeConflictedField) {
                    yield useLegacyField
                        ? InvalidMappedField.potentiallyUnmapped(name, partiallyUnmappedTypesByName(field, mappedIndices))
                        : CompactInvalidMappedField.potentiallyUnmapped(
                            name,
                            partiallyUnmappedTypesByDataType(field, mappedIndices),
                            indexDedupCache
                        );
                }
                // We only need to maintain mappedIndices for the legacy InvalidMappedType.
                yield new PotentiallyUnmappedSingleTypeEsField(field, useLegacyField ? mappedIndices : Set.of());
            }
        };
    }

    private static Map<String, Set<String>> partiallyUnmappedTypesByName(EsField field, Set<String> mappedIndices) {
        return field instanceof TypeConflictedField tcf ? tcf.getTypesToIndices() : Map.of(field.getDataType().typeName(), mappedIndices);
    }

    private static Map<DataType, Set<String>> partiallyUnmappedTypesByDataType(EsField field, Set<String> mappedIndices) {
        if (field instanceof TypeConflictedField tcf) {
            Map<DataType, Set<String>> result = new TreeMap<>();
            tcf.getTypesToIndices().forEach((typeName, indices) -> result.put(DataType.fromTypeName(typeName), indices));
            return result;
        }
        return Map.of(field.getDataType(), mappedIndices);
    }

    private static UnsupportedEsField unsupported(String name, IndexFieldCapabilities fc) {
        String originalType = fc.metricType() == TimeSeriesParams.MetricType.COUNTER ? "counter" : fc.type();
        return new UnsupportedEsField(name, List.of(originalType));
    }

    private static EsField conflictingTypes(
        String name,
        String fullName,
        List<IndexFields> indexFields,
        // If this isn't null, it also means we need to create CompactInvalidMappedField instead of InvalidMappedField.
        @Nullable Map<Set<String>, Set<String>> indexDedupCache
    ) {
        Map<DataType, Set<String>> typesToIndices = new TreeMap<>();
        for (IndexFields index : indexFields) {
            IndexFieldCapabilities fc = index.fields().get(fullName);
            if (fc != null) {
                DataType type = EsqlDataTypeRegistry.INSTANCE.fromEs(fc.type(), fc.metricType());
                if (type == UNSUPPORTED) {
                    return unsupported(name, fc);
                }
                typesToIndices.computeIfAbsent(type, _key -> new TreeSet<>()).add(index.response().getIndexName());
            }
        }
        if (indexDedupCache != null) {
            return CompactInvalidMappedField.mappedEverywhere(name, typesToIndices, indexDedupCache);
        }
        Map<String, Set<String>> stringKeyed = new TreeMap<>();
        typesToIndices.forEach((type, indices) -> stringKeyed.put(type.typeName(), indices));
        return new InvalidMappedField(name, stringKeyed);
    }

    private static boolean useCompactInvalidMappedField(@Nullable TransportVersion minTransportVersion) {
        return minTransportVersion != null && minTransportVersion.supports(CompactMultiTypeEsField.CompactMultiTypeEsField);
    }

    private static EsqlResolveFieldsRequest createResolveFieldRequest(
        IndicesOptions options,
        String index,
        @Nullable String projectRouting,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        boolean includeResolvedTo
    ) {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indices(Strings.commaDelimitedListToStringArray(index));
        request.fields(fieldNames.toArray(String[]::new));
        request.includeUnmapped(true);
        request.indexFilter(requestFilter);
        request.returnLocalAll(false);
        // lenient because we throw our own errors looking at the response e.g. if something was not resolved
        // also because this way security doesn't throw authorization exceptions but rather honors ignore_unavailable
        request.indicesOptions(options);
        // we ignore the nested data type fields starting with https://github.com/elastic/elasticsearch/pull/111495
        if (includeAllDimensions) {
            request.filters("-nested", "+dimension");
        } else {
            request.filters("-nested");
        }
        request.setMergeResults(false);
        request.includeResolvedTo(includeResolvedTo);
        request.projectRouting(projectRouting);
        return new EsqlResolveFieldsRequest(request);
    }

    public interface OriginalIndexExtractor {
        Map<String, List<String>> apply(String indexPattern, FieldCapabilitiesResponse fieldCapabilitiesResponse);
    }

    public static final OriginalIndexExtractor DO_NOT_GROUP = (indexPattern, fieldCapabilitiesResponse) -> Map.of();
}
