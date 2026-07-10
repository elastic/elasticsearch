/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.BoundsCheck;
import org.elasticsearch.foreign.MatrixSegment;
import org.elasticsearch.foreign.VectorSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.processing.Messager;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.tools.Diagnostic.Kind;

/**
 * Models one native-call bounds check to emit for a {@code MemorySegment} parameter, derived from
 * parameter annotations which are meta-annotated with {@code @BoundsCheck}.
 * All indices, including {@link #segParamIndex()}, are positions into the
 * enclosing method's parameter list (0-based).
 */
public sealed interface BoundsCheckModel {

    /** Index of the annotated {@code MemorySegment} parameter this check applies to. */
    int segParamIndex();

    /**
     * 1D linear shape, from {@code @VectorSegment}: the segment must be at least
     * {@code count * elementBits / 8} bytes. When {@code aligned}, the processor additionally emits
     * an {@code assert} that the segment's address is aligned to {@code elementBits / 8} bytes.
     *
     * @param segParamIndex index of the annotated {@code MemorySegment} parameter
     * @param countParamIndex index of the sibling parameter holding the element count
     * @param elementBits bits per element (whole-byte sizes are {@code elementBytes * 8})
     * @param aligned whether to additionally assert the segment is aligned to {@code elementBits / 8} bytes
     */
    record VectorSegmentCheck(int segParamIndex, int countParamIndex, int elementBits, boolean aligned) implements BoundsCheckModel {}

    /**
     * 2D shape, from {@code @MatrixSegment}: the segment must be at least {@code rows * rowBytes}
     * bytes, where {@code rowBytes} is either a direct sibling parameter ({@link #hasDirectRowBytes()})
     * or {@code cols * elementBits / 8}. When {@link #hasRowPitchBytes()}, rows are strided/padded
     * rather than packed contiguously: the required size becomes {@code rows * rowPitchBytes} instead,
     * and the processor additionally emits a check that {@code (rowPitchBytes < rowBytes)}. When
     * {@code aligned}, the processor additionally emits an {@code assert} that the segment's address
     * is aligned to {@code elementBits / 8} bytes (only possible when {@code colsParamIndex >= 0}).
     *
     * @param segParamIndex index of the annotated {@code MemorySegment} parameter
     * @param rowsParamIndex index of the sibling parameter holding the row count
     * @param colsParamIndex index of the sibling parameter holding the column count, or {@code -1}
     *        when {@code rowBytesParamIndex} is used instead
     * @param elementBits bits per element, meaningful only when {@code colsParamIndex >= 0}
     * @param rowBytesParamIndex index of the sibling parameter already holding the row size in
     *        bytes, or {@code -1} when {@code colsParamIndex}/{@code elementBits} are used instead
     * @param rowPitchBytesParamIndex index of the sibling parameter holding the actual per-row
     *        stride in bytes, or {@code -1} when rows are packed contiguously (no padding)
     * @param aligned whether to additionally assert the segment is aligned to {@code elementBits / 8} bytes
     */
    record MatrixSegmentCheck(
        int segParamIndex,
        int rowsParamIndex,
        int colsParamIndex,
        int elementBits,
        int rowBytesParamIndex,
        int rowPitchBytesParamIndex,
        boolean aligned
    ) implements BoundsCheckModel {

        /** True if the row size comes from a direct byte-count sibling parameter, not {@code cols * elementBits / 8}. */
        public boolean hasDirectRowBytes() {
            return rowBytesParamIndex >= 0;
        }

        /** True if rows are strided/padded — sized via {@code rowPitchBytes} rather than the packed row size. */
        public boolean hasRowPitchBytes() {
            return rowPitchBytesParamIndex >= 0;
        }
    }

    /**
     * Resolves {@code @BoundsCheck}-meta-annotated parameter annotations (currently
     * {@code @VectorSegment}/{@code @MatrixSegment}) on {@code method}'s parameters into a list of
     * {@link BoundsCheckModel}s, one entry per annotated parameter. Emits {@link Kind#ERROR}
     * diagnostics and returns {@code null} on any validation failure:
     * <ul>
     *     <li>a bounds-check annotation on a non-{@code MemorySegment} parameter;</li>
     *     <li>more than one bounds-check annotation on the same parameter;</li>
     *     <li>unresolved sibling parameter reference;</li>
     *     <li>incorrect sibling parameter type;</li>
     *     <li>incorrect attribute combination (e.g. two mutual exclusive attributes are both specified)</li>
     * </ul>
     */
    static List<BoundsCheckModel> from(ExecutableElement method, List<NativeType> paramTypes, Messager messager) {
        List<? extends VariableElement> params = method.getParameters();
        List<BoundsCheckModel> checks = new ArrayList<>();
        for (int i = 0; i < params.size(); i++) {
            VariableElement param = params.get(i);
            List<AnnotationMirror> boundsAnnotations = boundsAnnotationsOn(param);
            if (boundsAnnotations.isEmpty() == false) {
                if (boundsAnnotations.size() > 1) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Parameter ["
                            + param.getSimpleName()
                            + "] cannot combine multiple bounds-check annotations: "
                            + describeAnnotationTypes(boundsAnnotations),
                        param
                    );
                    return null;
                }
                BoundsCheckModel check = resolve(i, param, params, paramTypes, messager);
                if (check == null) {
                    return null;
                }
                checks.add(check);
            }
        }
        return checks;
    }

    /**
     * Finds annotations on {@code param} that are meta-annotated with {@code @BoundsCheck}.
     */
    private static List<AnnotationMirror> boundsAnnotationsOn(VariableElement param) {
        List<AnnotationMirror> result = new ArrayList<>();
        for (AnnotationMirror mirror : param.getAnnotationMirrors()) {
            TypeElement annotationType = (TypeElement) mirror.getAnnotationType().asElement();
            if (annotationType.getAnnotation(BoundsCheck.class) != null) {
                result.add(mirror);
            }
        }
        return result;
    }

    private static String describeAnnotationTypes(List<AnnotationMirror> mirrors) {
        return mirrors.stream()
            .map(mirror -> "@" + mirror.getAnnotationType().asElement().getSimpleName())
            .collect(Collectors.joining(", "));
    }

    /**
     * Resolves a bounds-check annotation present on {@code param}.
     * Returns {@code null} (with a {@link Kind#ERROR} emitted) on any validation failure.
     */
    private static BoundsCheckModel resolve(
        int segParamIndex,
        VariableElement param,
        List<? extends VariableElement> params,
        List<NativeType> paramTypes,
        Messager messager
    ) {
        if (paramTypes.get(segParamIndex) != NativeType.ADDRESS) {
            messager.printMessage(
                Kind.ERROR,
                "A bounds-check annotation can only be applied to a MemorySegment parameter, got [" + param.getSimpleName() + "]",
                param
            );
            return null;
        }
        VectorSegment vectorSegment = param.getAnnotation(VectorSegment.class);
        if (vectorSegment != null) {
            return resolveVectorSegmentCheck(segParamIndex, vectorSegment, param, params, paramTypes, messager);
        }
        MatrixSegment matrixSegment = param.getAnnotation(MatrixSegment.class);
        if (matrixSegment != null) {
            return resolveMatrixSegmentCheck(segParamIndex, matrixSegment, param, params, paramTypes, messager);
        }
        messager.printMessage(Kind.ERROR, "Unknown bounds-check annotation type", param);
        return null;
    }

    private static VectorSegmentCheck resolveVectorSegmentCheck(
        int segParamIndex,
        VectorSegment annotation,
        VariableElement param,
        List<? extends VariableElement> params,
        List<NativeType> paramTypes,
        Messager messager
    ) {
        int countIndex = resolveIntSiblingParam("@VectorSegment.countParam", annotation.countParam(), param, params, paramTypes, messager);
        if (countIndex < 0) {
            return null;
        }
        int elementBits = resolveElementBits("@VectorSegment", annotation.elementBytes(), annotation.elementBits(), param, messager);
        if (elementBits < 0) {
            return null;
        }
        if (annotation.aligned() && annotation.elementBytes() == 0) {
            messager.printMessage(
                Kind.ERROR,
                "@VectorSegment.aligned on parameter [" + param.getSimpleName() + "] requires 'elementBytes'",
                param
            );
            return null;
        }
        return new VectorSegmentCheck(segParamIndex, countIndex, elementBits, annotation.aligned());
    }

    private static MatrixSegmentCheck resolveMatrixSegmentCheck(
        int segParamIndex,
        MatrixSegment annotation,
        VariableElement param,
        List<? extends VariableElement> params,
        List<NativeType> paramTypes,
        Messager messager
    ) {
        int rowsIndex = resolveIntSiblingParam("@MatrixSegment.rowsParam", annotation.rowsParam(), param, params, paramTypes, messager);
        if (rowsIndex < 0) {
            return null;
        }
        int rowPitchBytesIndex = -1;
        if (annotation.rowPitchBytesParam().isEmpty() == false) {
            rowPitchBytesIndex = resolveIntSiblingParam(
                "@MatrixSegment.rowPitchBytesParam",
                annotation.rowPitchBytesParam(),
                param,
                params,
                paramTypes,
                messager
            );
            if (rowPitchBytesIndex < 0) {
                return null;
            }
        }
        boolean hasCols = annotation.colsParam().isEmpty() == false;
        boolean hasRowBytes = annotation.rowBytesParam().isEmpty() == false;
        if (hasCols == hasRowBytes) {
            messager.printMessage(
                Kind.ERROR,
                "@MatrixSegment on parameter [" + param.getSimpleName() + "] must set exactly one of 'colsParam' or 'rowBytesParam'",
                param
            );
            return null;
        }
        if (hasRowBytes) {
            if (annotation.elementBytes() != 0 || annotation.elementBits() != 0) {
                messager.printMessage(
                    Kind.ERROR,
                    "@MatrixSegment on parameter ["
                        + param.getSimpleName()
                        + "] cannot combine 'rowBytesParam' with 'elementBytes'/'elementBits'",
                    param
                );
                return null;
            }
            if (annotation.aligned()) {
                messager.printMessage(
                    Kind.ERROR,
                    "@MatrixSegment.aligned on parameter [" + param.getSimpleName() + "] requires 'colsParam' + 'elementBytes'",
                    param
                );
                return null;
            }
            int rowBytesIndex = resolveIntSiblingParam(
                "@MatrixSegment.rowBytesParam",
                annotation.rowBytesParam(),
                param,
                params,
                paramTypes,
                messager
            );
            if (rowBytesIndex < 0) {
                return null;
            }
            return new MatrixSegmentCheck(segParamIndex, rowsIndex, -1, 0, rowBytesIndex, rowPitchBytesIndex, false);
        }
        int colsIndex = resolveIntSiblingParam("@MatrixSegment.colsParam", annotation.colsParam(), param, params, paramTypes, messager);
        if (colsIndex < 0) {
            return null;
        }
        int elementBits = resolveElementBits("@MatrixSegment", annotation.elementBytes(), annotation.elementBits(), param, messager);
        if (elementBits < 0) {
            return null;
        }
        if (annotation.aligned() && annotation.elementBytes() == 0) {
            messager.printMessage(
                Kind.ERROR,
                "@MatrixSegment.aligned on parameter [" + param.getSimpleName() + "] requires 'elementBytes'",
                param
            );
            return null;
        }
        return new MatrixSegmentCheck(segParamIndex, rowsIndex, colsIndex, elementBits, -1, rowPitchBytesIndex, annotation.aligned());
    }

    /**
     * Resolves {@code paramName} against {@code params} by simple name and verifies it is an
     * {@code int}/{@code long} parameter. Returns the resolved index, or {@code -1} (with a
     * {@link Kind#ERROR} emitted) on failure.
     */
    private static int resolveIntSiblingParam(
        String attributeDescription,
        String paramName,
        VariableElement annotatedParam,
        List<? extends VariableElement> params,
        List<NativeType> paramTypes,
        Messager messager
    ) {
        for (int i = 0; i < params.size(); i++) {
            if (params.get(i).getSimpleName().contentEquals(paramName)) {
                NativeType type = paramTypes.get(i);
                if (type != NativeType.INT && type != NativeType.LONG) {
                    messager.printMessage(
                        Kind.ERROR,
                        attributeDescription + " parameter [" + paramName + "] must be int or long, got: " + type,
                        annotatedParam
                    );
                    return -1;
                }
                return i;
            }
        }
        messager.printMessage(Kind.ERROR, attributeDescription + " references unknown parameter [" + paramName + "]", annotatedParam);
        return -1;
    }

    /**
     * Verifies exactly one of {@code elementBytes}/{@code elementBits} is set (positive) and returns
     * the element size normalized to bits (whole-byte sizes become {@code elementBytes * 8}). Returns
     * {@code -1} (with a {@link Kind#ERROR} emitted) on failure.
     */
    private static int resolveElementBits(
        String annotationName,
        int elementBytes,
        int elementBits,
        VariableElement annotatedParam,
        Messager messager
    ) {
        boolean hasBytes = elementBytes != 0;
        boolean hasBits = elementBits != 0;
        if (hasBytes == hasBits) {
            messager.printMessage(
                Kind.ERROR,
                annotationName
                    + " on parameter ["
                    + annotatedParam.getSimpleName()
                    + "] requires exactly one of 'elementBytes' or 'elementBits'",
                annotatedParam
            );
            return -1;
        }
        if (hasBytes) {
            if (elementBytes < 0) {
                messager.printMessage(
                    Kind.ERROR,
                    annotationName + ".elementBytes on parameter [" + annotatedParam.getSimpleName() + "] must be positive",
                    annotatedParam
                );
                return -1;
            }
            return elementBytes * 8;
        }
        if (elementBits < 0) {
            messager.printMessage(
                Kind.ERROR,
                annotationName + ".elementBits on parameter [" + annotatedParam.getSimpleName() + "] must be positive",
                annotatedParam
            );
            return -1;
        }
        return elementBits;
    }
}
