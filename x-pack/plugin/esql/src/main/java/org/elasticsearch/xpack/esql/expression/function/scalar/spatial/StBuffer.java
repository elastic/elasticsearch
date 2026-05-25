/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

public class StBuffer extends SpatialDocValuesFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StBuffer", StBuffer::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(StBuffer.class).ternary(StBuffer::new).name("st_buffer");

    /**
     * Adds the optional {@code options} {@link MapExpression} (quad_segs, endcap, join, mitre_limit)
     * to the wire format of {@link StBuffer}.
     */
    public static final TransportVersion ESQL_ST_BUFFER_OPTIONS = TransportVersion.fromName("esql_st_buffer_options");

    static final String QUAD_SEGS_OPTION = "quad_segs";
    static final String ENDCAP_OPTION = "endcap";
    static final String JOIN_OPTION = "join";
    static final String MITRE_LIMIT_OPTION = "mitre_limit";
    private static final String OPTIONS_APPLIES_TO = """
        {"serverless": "preview", "stack": "preview 9.5.0"}""";

    // TreeMap so the keys iterate in alphabetical order; this makes "expected one of [...]"
    // error messages deterministic across JVM hash seeds, which is important for tests.
    public static final Map<String, DataType> ALLOWED_OPTIONS = new TreeMap<>(
        Map.of(QUAD_SEGS_OPTION, INTEGER, ENDCAP_OPTION, KEYWORD, JOIN_OPTION, KEYWORD, MITRE_LIMIT_OPTION, DOUBLE)
    );

    private static final SpatialGeometryBlockProcessor processor = new SpatialGeometryBlockProcessor(UNSPECIFIED, StBuffer::bufferOp);
    private static final SpatialGeometryBlockProcessor geoProcessor = new SpatialGeometryBlockProcessor(GEO, StBuffer::bufferOp);
    private static final SpatialGeometryBlockProcessor cartesianProcessor = new SpatialGeometryBlockProcessor(
        CARTESIAN,
        StBuffer::bufferOp
    );

    private static final BufferParameters DEFAULT_BUFFER_PARAMETERS = new BufferParameters();

    /**
     * Wraps {@link BufferOp#bufferOp} to return the original geometry when distance is zero,
     * matching PostGIS behavior. JTS returns POLYGON EMPTY for points/lines with zero distance,
     * but the expected behavior is to return the original geometry unchanged.
     * <p>This 2-arg variant is used when no buffer options are supplied (still kept as the
     * lambda for the static {@link SpatialGeometryBlockProcessor} fields above, even though
     * the new evaluator path always supplies an explicit {@link BufferParameters}).</p>
     */
    private static Geometry bufferOp(Geometry geometry, double distance) {
        return bufferOp(geometry, distance, DEFAULT_BUFFER_PARAMETERS);
    }

    private static Geometry bufferOp(Geometry geometry, double distance, BufferParameters bufferParameters) {
        if (distance == 0) {
            return geometry;
        }
        return BufferOp.bufferOp(geometry, distance, bufferParameters);
    }

    private final Expression geometry;
    private final Expression distance;
    private final Expression options;

    @FunctionInfo(
        returnType = { "geo_shape", "cartesian_shape" },
        description = "Computes a buffer area around the input geometry at the specified distance. "
            + "The distance is in the units of the input spatial reference system. "
            + "Positive distances expand the geometry, negative distances shrink it. "
            + "A distance of zero will return the input geometry unchanged. "
            + "Points and lines become polygons when buffered, unless a zero distance is provided.",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        examples = {
            @Example(file = "spatial-jts", tag = "st_buffer"),
            @Example(
                description = "The optional `options` argument can configure end caps and join styles. "
                    + "Combining `endcap=flat` with `join=mitre` produces sharp corners on a buffered L-shape:",
                file = "spatial-jts",
                tag = "st_buffer_mitre_flat"
            ) },
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StBuffer(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression geometry,
        @Param(
            name = "distance",
            type = { "double", "float", "long", "integer" },
            description = "Buffer distance in the units of the input spatial reference system"
        ) Expression distance,
        @MapParam(
            name = "options",
            description = "(Optional) ST_BUFFER additional options like `quad_segs`, `endcap`, `join` and `mitre_limit`.",
            applies_to = OPTIONS_APPLIES_TO,
            params = {
                @MapParam.MapParamEntry(
                    name = QUAD_SEGS_OPTION,
                    type = "integer",
                    valueHint = { "8" },
                    description = "Number of line segments used to approximate a quarter circle. Defaults to 8.",
                    applies_to = OPTIONS_APPLIES_TO
                ),
                @MapParam.MapParamEntry(
                    name = ENDCAP_OPTION,
                    type = "keyword",
                    valueHint = { "round", "flat", "square" },
                    description = "End cap style for buffering linear geometries. Defaults to round.",
                    applies_to = OPTIONS_APPLIES_TO
                ),
                @MapParam.MapParamEntry(
                    name = JOIN_OPTION,
                    type = "keyword",
                    valueHint = { "round", "mitre", "bevel" },
                    description = "Join style for buffering. Defaults to round.",
                    applies_to = OPTIONS_APPLIES_TO
                ),
                @MapParam.MapParamEntry(
                    name = MITRE_LIMIT_OPTION,
                    type = "double",
                    valueHint = { "5.0" },
                    description = "Mitre ratio limit, only meaningful when join=mitre. Defaults to 5.0.",
                    applies_to = OPTIONS_APPLIES_TO
                ) },
            optional = true
        ) Expression options
    ) {
        this(source, geometry, distance, options, false);
    }

    private StBuffer(Source source, Expression geometry, Expression distance, Expression options, boolean spatialDocValues) {
        super(source, options == null ? List.of(geometry, distance) : List.of(geometry, distance, options), spatialDocValues);
        this.geometry = geometry;
        this.distance = distance;
        this.options = options;
    }

    private StBuffer(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().supports(ESQL_ST_BUFFER_OPTIONS) ? in.readOptionalNamedWriteable(Expression.class) : null,
            false
        );
    }

    @Override
    public DataType dataType() {
        if (distance.dataType() == DataType.NULL || geometry.dataType() == DataType.NULL) {
            return DataType.NULL;
        }
        return DataType.isSpatialGeo(geometry.dataType()) ? DataType.GEO_SHAPE : DataType.CARTESIAN_SHAPE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StBuffer(source(), newChildren.get(0), newChildren.get(1), newChildren.size() > 2 ? newChildren.get(2) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StBuffer::new, geometry, distance, options);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(geometry);
        out.writeNamedWriteable(distance);
        if (out.getTransportVersion().supports(ESQL_ST_BUFFER_OPTIONS)) {
            out.writeOptionalNamedWriteable(options);
        }
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StBuffer(source(), geometry, distance, options, useDocValues);
    }

    @Override
    public Expression spatialField() {
        return geometry;
    }

    Expression distance() {
        return distance;
    }

    Expression options() {
        return options;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        TypeResolution spatialResolved = isSpatial(geometry, sourceText(), FIRST);
        if (spatialResolved.unresolved()) {
            return spatialResolved;
        }
        TypeResolution distanceResolved = isType(
            distance,
            t -> t == DOUBLE || t == FLOAT || t == LONG || t == INTEGER,
            sourceText(),
            SECOND,
            "double",
            "float",
            "long",
            "integer"
        );
        if (distanceResolved.unresolved()) {
            return distanceResolved;
        }
        return Options.resolve(options, source(), THIRD, ALLOWED_OPTIONS);
    }

    /**
     * Build a JTS {@link BufferParameters} from the (possibly null) {@code options} expression.
     * Options must be a foldable {@link MapExpression} of literal entries.
     */
    BufferParameters bufferParameters() {
        BufferParameters bp = new BufferParameters();
        if (options == null) {
            return bp;
        }
        if (options instanceof MapExpression == false) {
            // Should already be caught by resolveType, but guard defensively.
            throw new InvalidArgumentException("options for st_buffer must be a literal map expression");
        }
        MapExpression mapOptions = (MapExpression) options;
        for (EntryExpression entry : mapOptions.entryExpressions()) {
            // Entry keys and values are always Literals here: the parser rejects non-literal
            // map values syntactically, and Options.resolve enforces the same invariant during
            // resolveType. The casts below are therefore safe.
            String name = BytesRefs.toString(((Literal) entry.key()).value());
            Object value = ((Literal) entry.value()).value();
            switch (name) {
                case QUAD_SEGS_OPTION -> bp.setQuadrantSegments(((Number) value).intValue());
                case ENDCAP_OPTION -> bp.setEndCapStyle(parseEndCap(BytesRefs.toString(value)));
                case JOIN_OPTION -> bp.setJoinStyle(parseJoin(BytesRefs.toString(value)));
                case MITRE_LIMIT_OPTION -> bp.setMitreLimit(((Number) value).doubleValue());
                default -> throw new InvalidArgumentException(
                    "Invalid option [" + name + "] in [" + sourceText() + "], expected one of " + ALLOWED_OPTIONS.keySet()
                );
            }
        }
        return bp;
    }

    private static int parseEndCap(String s) {
        return switch (s.toLowerCase(Locale.ROOT)) {
            case "round" -> BufferParameters.CAP_ROUND;
            case "flat", "butt" -> BufferParameters.CAP_FLAT;
            case "square" -> BufferParameters.CAP_SQUARE;
            default -> throw new InvalidArgumentException(
                "Invalid value [" + s + "] for option [" + ENDCAP_OPTION + "], expected one of [round, flat, square]"
            );
        };
    }

    private static int parseJoin(String s) {
        return switch (s.toLowerCase(Locale.ROOT)) {
            case "round" -> BufferParameters.JOIN_ROUND;
            case "mitre", "miter" -> BufferParameters.JOIN_MITRE;
            case "bevel" -> BufferParameters.JOIN_BEVEL;
            default -> throw new InvalidArgumentException(
                "Invalid value [" + s + "] for option [" + JOIN_OPTION + "], expected one of [round, mitre, bevel]"
            );
        };
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory geometryEvaluator = toEvaluator.apply(geometry);

        if (distance.foldable() == false) {
            throw new IllegalArgumentException("distance must be foldable");
        }
        var distanceExpression = distance.fold(toEvaluator.foldCtx());
        double inputDistance = getInputDistance(distanceExpression);
        BufferParameters bufferParameters = bufferParameters();
        if (spatialDocValues && geometry.dataType() == DataType.GEO_POINT) {
            return new StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputDistance,
                bufferParameters
            );
        } else if (spatialDocValues && geometry.dataType() == DataType.CARTESIAN_POINT) {
            return new StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputDistance,
                bufferParameters
            );
        }
        return new StBufferNonFoldableGeometryAndFoldableDistanceEvaluator.Factory(
            source(),
            geometryEvaluator,
            inputDistance,
            bufferParameters
        );
    }

    @Override
    public boolean foldable() {
        return geometry.foldable() && distance.foldable() && (options == null || options.foldable());
    }

    @Override
    public Object fold(FoldContext foldCtx) {
        var distanceExpression = distance.fold(foldCtx);
        double inputDistance = getInputDistance(distanceExpression);
        BufferParameters bp = bufferParameters();
        Object input = geometry.fold(foldCtx);
        return switch (input) {
            case null -> null;
            case List<?> list -> {
                Geometry jts = processor.asJtsGeometry(list);
                yield UNSPECIFIED.jtsGeometryToWkb(bufferOp(jts, inputDistance, bp));
            }
            case BytesRef inputGeometry -> {
                try {
                    Geometry jts = UNSPECIFIED.wkbToJtsGeometry(inputGeometry);
                    yield UNSPECIFIED.jtsGeometryToWkb(bufferOp(jts, inputDistance, bp));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("could not parse the geometry expression: " + e);
                }
            }
            default -> throw new IllegalArgumentException("unsupported block type: " + input.getClass().getSimpleName());
        };
    }

    @Evaluator(extraName = "NonFoldableGeometryAndFoldableDistance", warnExceptions = { IllegalArgumentException.class })
    static void processNonFoldableGeometryAndConstantDistance(
        BytesRefBlock.Builder builder,
        @Position int p,
        BytesRefBlock geometry,
        @Fixed double distance,
        @Fixed(includeInToString = false) BufferParameters bufferParameters
    ) {
        if (geometry.getValueCount(p) < 1) {
            builder.appendNull();
        } else {
            Geometry jts = processor.asJtsGeometry(geometry, p);
            Geometry result = bufferOp(jts, distance, bufferParameters);
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(result));
        }
    }

    @Evaluator(
        extraName = "NonFoldableGeoPointDocValuesAndFoldableDistance",
        warnExceptions = { IllegalArgumentException.class, IOException.class }
    )
    static void processGeoPointDocValuesAndConstantDistance(
        BytesRefBlock.Builder builder,
        @Position int p,
        LongBlock point,
        @Fixed double distance,
        @Fixed(includeInToString = false) BufferParameters bufferParameters
    ) throws IOException {
        if (point.getValueCount(p) < 1) {
            builder.appendNull();
        } else {
            Geometry jts = geoProcessor.asJtsMultiPoint(point, p, GEO::longAsPoint);
            Geometry result = bufferOp(jts, distance, bufferParameters);
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(result));
        }
    }

    @Evaluator(
        extraName = "NonFoldableCartesianPointDocValuesAndFoldableDistance",
        warnExceptions = { IllegalArgumentException.class, IOException.class }
    )
    static void processCartesianPointDocValuesAndConstantDistance(
        BytesRefBlock.Builder builder,
        @Position int p,
        LongBlock left,
        @Fixed double distance,
        @Fixed(includeInToString = false) BufferParameters bufferParameters
    ) throws IOException {
        if (left.getValueCount(p) < 1) {
            builder.appendNull();
        } else {
            Geometry jts = cartesianProcessor.asJtsMultiPoint(left, p, CARTESIAN::longAsPoint);
            Geometry result = bufferOp(jts, distance, bufferParameters);
            builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(result));
        }
    }

    static double getInputDistance(Object distanceExpression) {
        if (distanceExpression instanceof Number number) {
            return number.doubleValue();
        }
        throw new IllegalArgumentException("distance for st_buffer must be an integer or floating-point number");
    }
}
