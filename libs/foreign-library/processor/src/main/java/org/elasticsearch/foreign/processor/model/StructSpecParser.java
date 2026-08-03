/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import java.lang.foreign.MemoryLayout;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.RecordComponentElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic.Kind;

/**
 * Parses {@code @StructSpecification}-annotated types into {@link StructModel} instances. A single
 * walk over the type's members produces the field <em>shape</em> (name, type, getter/setter) once,
 * plus the resolved absolute byte offset of every field for every supported platform. Those offsets
 * are turned into one {@link MemoryLayout} per platform, and platforms sharing an identical layout
 * are collapsed into a single {@link StructLayoutModel}.
 *
 * <p>All annotation-processing API usage for struct building is concentrated here, keeping the model
 * types themselves as plain data records.
 */
class StructSpecParser {

    public static final String ARRAY_FIELD_FQN = org.elasticsearch.foreign.ArrayField.class.getName();
    private static final String STRUCT_SPECIFICATION_FQN = org.elasticsearch.foreign.StructSpecification.class.getName();
    private static final String OFFSET_FQN = "org.elasticsearch.foreign.Offset";
    private static final String OFFSET_LIST_FQN = "org.elasticsearch.foreign.Offset.List";
    private static final String PADDING_FQN = "org.elasticsearch.foreign.Padding";
    private static final String PADDING_LIST_FQN = "org.elasticsearch.foreign.Padding.List";
    private static final String STRUCT_SIZE_FQN = "org.elasticsearch.foreign.StructSize";
    private static final String STRUCT_SIZE_LIST_FQN = "org.elasticsearch.foreign.StructSize.List";

    /**
     * Builds the {@link StructModel} for a {@code @StructSpecification} record. Record components are
     * scalar getters laid out in declaration order; {@code @Offset}/{@code @Padding} on a component
     * and {@code @StructSize} on the record are honoured exactly as for interfaces. Emits errors for
     * unsupported component types and returns {@code null} if any error was emitted.
     */
    static StructModel fromRecord(TypeElement typeElement, Set<String> supportedPlatforms, Messager messager) {
        String typeSimpleName = typeElement.getSimpleName().toString();
        boolean isSparse = readSparseFlag(typeElement);

        List<AnnotationMirror> structSizeMirrors = ModelUtil.collectRepeatableAnnotations(
            typeElement,
            STRUCT_SIZE_FQN,
            STRUCT_SIZE_LIST_FQN
        );

        LayoutBuilder layout = new LayoutBuilder(supportedPlatforms);
        boolean error = validateModeStructSize(typeSimpleName, typeElement, isSparse, structSizeMirrors, messager);

        for (RecordComponentElement component : typeElement.getRecordComponents()) {
            NativeType fieldType = ModelUtil.classifyType(component.asType());
            if (fieldType == null
                || fieldType == NativeType.VOID
                || fieldType == NativeType.STRING
                || fieldType == NativeType.ADDRESSABLE) {
                messager.printMessage(
                    Kind.ERROR,
                    "Unsupported field type '"
                        + component.asType()
                        + "' on component '"
                        + component.getSimpleName()
                        + "' of @StructSpecification record '"
                        + typeSimpleName
                        + "'",
                    component
                );
                error = true;
                continue;
            }
            String name = component.getSimpleName().toString();
            StructFieldModel field = new ScalarFieldModel(name, fieldType, true, false);
            List<AnnotationMirror> offsetMirrors = ModelUtil.collectRepeatableAnnotations(component, OFFSET_FQN, OFFSET_LIST_FQN);
            List<AnnotationMirror> paddingMirrors = ModelUtil.collectRepeatableAnnotations(component, PADDING_FQN, PADDING_LIST_FQN);
            error |= placeNewField(
                layout,
                field,
                isSparse,
                offsetMirrors,
                paddingMirrors,
                supportedPlatforms,
                typeSimpleName,
                component,
                messager
            );
        }

        Map<String, Long> byteSizes = resolveByteSizes(
            layout,
            isSparse,
            structSizeMirrors,
            supportedPlatforms,
            typeSimpleName,
            typeElement,
            messager
        );
        if (byteSizes == null) {
            error = true;
        }

        if (error) {
            return null;
        }
        return buildStructModel(typeSimpleName, layout, byteSizes, supportedPlatforms, /* isRecord */ true);
    }

    /**
     * Builds the {@link StructModel} for a {@code @StructSpecification} interface. Walks the abstract
     * methods once, merging a getter/setter pair (which must be declared adjacently) into a single
     * field, validating {@code @ArrayField} length references, and placing every field at its resolved
     * absolute offset for each supported platform. Returns {@code null} on any error.
     */
    static StructModel fromInterface(
        TypeElement typeElement,
        List<String> priorStructNames,
        Set<String> supportedPlatforms,
        ProcessingEnvironment env,
        Messager messager
    ) {
        String typeSimpleName = typeElement.getSimpleName().toString();
        boolean isSparse = readSparseFlag(typeElement);

        List<AnnotationMirror> structSizeMirrors = ModelUtil.collectRepeatableAnnotations(
            typeElement,
            STRUCT_SIZE_FQN,
            STRUCT_SIZE_LIST_FQN
        );
        boolean error = validateModeStructSize(typeSimpleName, typeElement, isSparse, structSizeMirrors, messager);

        LayoutBuilder layout = new LayoutBuilder(supportedPlatforms);
        List<String> scalarFieldNames = new ArrayList<>();

        // Abstract instance methods in declaration order; a field is one such method (a getter or
        // setter) or two adjacent ones sharing a name (a getter/setter pair).
        List<ExecutableElement> methods = new ArrayList<>();
        for (var enclosedMember : typeElement.getEnclosedElements()) {
            if (enclosedMember.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) enclosedMember;
            var mods = method.getModifiers();
            if (mods.contains(Modifier.DEFAULT) || mods.contains(Modifier.STATIC)) {
                continue;
            }
            methods.add(method);
        }

        for (int i = 0; i < methods.size();) {
            ExecutableElement first = methods.get(i);
            String name = first.getSimpleName().toString();

            // A field's complement accessor, if any, is the immediately following same-named method.
            ExecutableElement second = (i + 1 < methods.size() && methods.get(i + 1).getSimpleName().contentEquals(name))
                ? methods.get(i + 1)
                : null;
            int consumed = second == null ? 1 : 2;
            i += consumed;

            // A same-named field placed earlier (non-adjacent accessors) would reorder the layout.
            if (layout.seen(name)) {
                messager.printMessage(
                    Kind.ERROR,
                    "getter and setter for '" + name + "' on '" + typeSimpleName + "' must be declared adjacent",
                    first
                );
                error = true;
                continue;
            }

            // Parse the whole field (both accessors) before placing it.
            StructFieldModel field = parseField(first, second, typeSimpleName, priorStructNames, env, messager);
            if (field == null) {
                error = true;
                continue;
            }
            layout.markSeen(name);
            if (field instanceof ScalarFieldModel) {
                scalarFieldNames.add(name);
            }

            // Layout annotations are only permitted on the first-declared accessor.
            List<AnnotationMirror> offsetMirrors = ModelUtil.collectRepeatableAnnotations(first, OFFSET_FQN, OFFSET_LIST_FQN);
            List<AnnotationMirror> paddingMirrors = ModelUtil.collectRepeatableAnnotations(first, PADDING_FQN, PADDING_LIST_FQN);
            if (second != null
                && (ModelUtil.collectRepeatableAnnotations(second, OFFSET_FQN, OFFSET_LIST_FQN).isEmpty() == false
                    || ModelUtil.collectRepeatableAnnotations(second, PADDING_FQN, PADDING_LIST_FQN).isEmpty() == false)) {
                messager.printMessage(
                    Kind.ERROR,
                    "@Offset/@Padding on '" + name + "' in '" + typeSimpleName + "' must be on the first-declared accessor of the field",
                    second
                );
                error = true;
            }

            error |= placeNewField(
                layout,
                field,
                isSparse,
                offsetMirrors,
                paddingMirrors,
                supportedPlatforms,
                typeSimpleName,
                first,
                messager
            );
        }

        // Every @ArrayField's lengthField must name a real scalar field on this same struct.
        for (StructFieldModel fm : layout.fields()) {
            if (fm instanceof ArrayFieldModel array && scalarFieldNames.contains(array.lengthFieldName()) == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@ArrayField on '"
                        + array.name()
                        + "' references lengthField '"
                        + array.lengthFieldName()
                        + "' which is not a scalar field on '"
                        + typeSimpleName
                        + "'",
                    typeElement
                );
                error = true;
            }
        }

        Map<String, Long> byteSizes = resolveByteSizes(
            layout,
            isSparse,
            structSizeMirrors,
            supportedPlatforms,
            typeSimpleName,
            typeElement,
            messager
        );
        if (byteSizes == null) {
            error = true;
        }

        if (error) {
            return null;
        }
        return buildStructModel(typeSimpleName, layout, byteSizes, supportedPlatforms, /* isRecord */ false);
    }

    // --- Layout accumulation ---

    /**
     * Accumulates the field shapes (identical across platforms, recorded once) and each field's
     * resolved absolute offset per platform (parallel to the field list) while fields are placed.
     */
    private static final class LayoutBuilder {
        private final List<StructFieldModel> fields = new ArrayList<>();
        private final Map<String, List<Long>> offsets = new LinkedHashMap<>();
        private final Map<String, Long> cursor = new LinkedHashMap<>();
        private final Set<String> seenNames = new LinkedHashSet<>();

        LayoutBuilder(Set<String> platforms) {
            for (String p : platforms) {
                cursor.put(p, 0L);
                offsets.put(p, new ArrayList<>());
            }
        }

        boolean seen(String name) {
            return seenNames.contains(name);
        }

        void markSeen(String name) {
            seenNames.add(name);
        }

        long cursor(String platform) {
            return cursor.get(platform);
        }

        /** Records a field's shape once (shared across platforms). */
        void addField(StructFieldModel field) {
            fields.add(field);
        }

        /** Records the just-added field's resolved offset on one platform and advances that cursor. */
        void placeOffset(String platform, long offset, long newCursor) {
            offsets.get(platform).add(offset);
            cursor.put(platform, newCursor);
        }

        /** Field shapes in declaration order — identical across platforms. */
        List<StructFieldModel> fields() {
            return fields;
        }

        /** Absolute offsets for one platform, index-aligned with {@link #fields()}. */
        List<Long> offsetsFor(String platform) {
            return offsets.get(platform);
        }
    }

    /**
     * Places a field on every platform: records its shape once and resolves its absolute offset per
     * platform. Validates the field's layout annotations against the struct mode (dense vs sparse)
     * and, for sparse, that the offset does not overlap the previous field. Returns {@code true} if
     * any error was emitted.
     */
    private static boolean placeNewField(
        LayoutBuilder layout,
        StructFieldModel field,
        boolean isSparse,
        List<AnnotationMirror> offsetMirrors,
        List<AnnotationMirror> paddingMirrors,
        Set<String> supportedPlatforms,
        String typeSimpleName,
        Element reportElement,
        Messager messager
    ) {
        String name = field.name();
        long size = FieldLayouts.byteSize(field);
        boolean error = false;
        layout.addField(field);

        if (isSparse) {
            if (paddingMirrors.isEmpty() == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@Padding on '" + name + "' in '" + typeSimpleName + "' is not allowed in sparse mode (@Offset should be used instead)",
                    reportElement
                );
                error = true;
            }
            if (offsetMirrors.isEmpty()) {
                messager.printMessage(
                    Kind.ERROR,
                    "Field '" + name + "' in sparse @StructSpecification '" + typeSimpleName + "' must have an @Offset annotation",
                    reportElement
                );
                error = true;
            }
            Map<String, Integer> offsets = offsetMirrors.isEmpty()
                ? null
                : resolvePerPlatform(offsetMirrors, supportedPlatforms, "Offset", reportElement, messager);
            if (offsets == null) {
                // Missing @Offset or a resolution error was already reported; place at the current
                // cursor so later fields still validate, but report the failure.
                for (String p : supportedPlatforms) {
                    long cursor = layout.cursor(p);
                    layout.placeOffset(p, cursor, cursor + size);
                }
                return true;
            }
            for (String p : supportedPlatforms) {
                long offset = offsets.get(p);
                long cursor = layout.cursor(p);
                if (offset < cursor) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@Offset "
                            + offset
                            + " for field '"
                            + name
                            + "' in '"
                            + typeSimpleName
                            + "' overlaps the end of the previous field at "
                            + cursor,
                        reportElement
                    );
                    error = true;
                }
                layout.placeOffset(p, offset, offset + size);
            }
        } else {
            if (offsetMirrors.isEmpty() == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@Offset on '"
                        + name
                        + "' in '"
                        + typeSimpleName
                        + "' is not allowed in dense mode (set sparse = true on @StructSpecification to enable @Offset)",
                    reportElement
                );
                error = true;
            }
            Map<String, Integer> paddings = paddingMirrors.isEmpty()
                ? null
                : resolvePerPlatform(paddingMirrors, supportedPlatforms, "Padding", reportElement, messager);
            if (paddings == null && paddingMirrors.isEmpty() == false) {
                error = true; // resolution error already reported
            }
            long align = FieldLayouts.alignment(field);
            for (String p : supportedPlatforms) {
                long cursor = layout.cursor(p);
                long pad;
                if (paddings != null) {
                    pad = paddings.get(p);
                } else {
                    pad = (cursor % align == 0) ? 0 : (align - cursor % align);
                }
                long offset = cursor + pad;
                layout.placeOffset(p, offset, offset + size);
            }
        }

        return error;
    }

    /**
     * Resolves the total byte size for each platform: the running cursor in dense mode, or the
     * resolved {@code @StructSize} in sparse mode (also validating it is not smaller than the struct's
     * content). Returns {@code null} if any error was emitted.
     */
    private static Map<String, Long> resolveByteSizes(
        LayoutBuilder layout,
        boolean isSparse,
        List<AnnotationMirror> structSizeMirrors,
        Set<String> supportedPlatforms,
        String typeSimpleName,
        Element reportElement,
        Messager messager
    ) {
        Map<String, Long> byteSizes = new LinkedHashMap<>();
        if (isSparse == false) {
            for (String p : supportedPlatforms) {
                byteSizes.put(p, layout.cursor(p));
            }
            return byteSizes;
        }
        if (structSizeMirrors.isEmpty()) {
            // Missing @StructSize already reported by validateModeStructSize.
            return null;
        }
        Map<String, Integer> sizes = resolvePerPlatform(structSizeMirrors, supportedPlatforms, "StructSize", reportElement, messager);
        if (sizes == null) {
            return null;
        }
        boolean error = false;
        for (String p : supportedPlatforms) {
            long total = sizes.get(p);
            long cursor = layout.cursor(p);
            if (total < cursor) {
                messager.printMessage(
                    Kind.ERROR,
                    "@StructSize " + total + " in '" + typeSimpleName + "' is smaller than the end of the last field at " + cursor,
                    reportElement
                );
                error = true;
            }
            byteSizes.put(p, total);
        }
        return error ? null : byteSizes;
    }

    /**
     * Builds one {@link MemoryLayout} per platform from the shared field shapes and that platform's
     * resolved offsets, then collapses platforms with an identical layout into a single
     * {@link StructLayoutModel}. Groups (and the platforms within them) come out in {@code Platform}
     * ordinal order because {@code supportedPlatforms} is iterated in that order.
     */
    private static StructModel buildStructModel(
        String typeSimpleName,
        LayoutBuilder layout,
        Map<String, Long> byteSizes,
        Set<String> supportedPlatforms,
        boolean isRecord
    ) {
        List<StructFieldModel> fields = List.copyOf(layout.fields());
        Map<MemoryLayout, List<String>> platformsByLayout = new LinkedHashMap<>();
        for (String p : supportedPlatforms) {
            MemoryLayout memoryLayout = FieldLayouts.structLayout(fields, layout.offsetsFor(p), byteSizes.get(p));
            platformsByLayout.computeIfAbsent(memoryLayout, k -> new ArrayList<>()).add(p);
        }
        List<StructLayoutModel> layouts = new ArrayList<>();
        for (var entry : platformsByLayout.entrySet()) {
            layouts.add(new StructLayoutModel(List.copyOf(entry.getValue()), entry.getKey()));
        }
        return isRecord
            ? new StructRecordModel(typeSimpleName, fields, layouts)
            : new StructInterfaceModel(typeSimpleName, fields, layouts);
    }

    // --- Mode validation ---

    /** Validates the {@code @StructSize} presence rule for the struct's mode. Returns {@code true} on error. */
    private static boolean validateModeStructSize(
        String typeSimpleName,
        Element reportElement,
        boolean isSparse,
        List<AnnotationMirror> structSizeMirrors,
        Messager messager
    ) {
        if (isSparse && structSizeMirrors.isEmpty()) {
            messager.printMessage(
                Kind.ERROR,
                "Sparse @StructSpecification '" + typeSimpleName + "' must have a @StructSize annotation",
                reportElement
            );
            return true;
        }
        if (isSparse == false && structSizeMirrors.isEmpty() == false) {
            messager.printMessage(
                Kind.ERROR,
                "@StructSize on '"
                    + typeSimpleName
                    + "' is not allowed in dense mode (set sparse = true on @StructSpecification to enable @StructSize)",
                reportElement
            );
            return true;
        }
        return false;
    }

    // --- Per-platform annotation resolution ---

    /**
     * Resolves a list of repeated layout annotation mirrors (e.g. {@code @Offset}/{@code @Padding}/
     * {@code @StructSize}) to a value per supported platform. A bare annotation (empty
     * {@code platforms}) is the universal fallback; per-platform entries override it. Validates that
     * at most one universal is present, no platform is covered twice, and every supported platform
     * resolves. Returns {@code null} on any error (already emitted).
     */
    private static Map<String, Integer> resolvePerPlatform(
        List<AnnotationMirror> mirrors,
        Set<String> supportedPlatforms,
        String annotationName,
        Element reportElement,
        Messager messager
    ) {
        Integer universal = null;
        Map<String, Integer> perPlatform = new LinkedHashMap<>();
        boolean error = false;

        for (AnnotationMirror mirror : mirrors) {
            Integer value = ModelUtil.annotationIntValue(mirror, "value");
            Set<String> platforms = ModelUtil.extractPlatforms(mirror);
            if (platforms.isEmpty()) {
                if (universal != null) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Duplicate universal @" + annotationName + " on '" + reportElement.getSimpleName() + "'",
                        reportElement
                    );
                    error = true;
                } else {
                    universal = value;
                }
            } else {
                for (String platform : platforms) {
                    if (perPlatform.containsKey(platform)) {
                        messager.printMessage(
                            Kind.ERROR,
                            "Overlapping @"
                                + annotationName
                                + " for platform '"
                                + platform
                                + "' on '"
                                + reportElement.getSimpleName()
                                + "'",
                            reportElement
                        );
                        error = true;
                    } else {
                        perPlatform.put(platform, value);
                    }
                }
            }
        }
        if (error) {
            return null;
        }

        Map<String, Integer> resolved = new LinkedHashMap<>();
        for (String platform : supportedPlatforms) {
            if (perPlatform.containsKey(platform)) {
                resolved.put(platform, perPlatform.get(platform));
            } else if (universal != null) {
                resolved.put(platform, universal);
            } else {
                messager.printMessage(
                    Kind.ERROR,
                    "@" + annotationName + " does not resolve for platform '" + platform + "' on '" + reportElement.getSimpleName() + "'",
                    reportElement
                );
                error = true;
            }
        }
        return error ? null : resolved;
    }

    // --- Sparse flag reading ---

    private static boolean readSparseFlag(TypeElement typeElement) {
        AnnotationMirror structSpecMirror = ModelUtil.findAnnotationMirror(typeElement, STRUCT_SPECIFICATION_FQN);
        if (structSpecMirror == null) {
            return false;
        }
        for (var entry : structSpecMirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals("sparse")) {
                Object val = entry.getValue().getValue();
                return val instanceof Boolean b ? b : false;
            }
        }
        return false;
    }

    // --- Field parsing ---

    /**
     * Parses a whole interface field from its accessor(s): a single method, or a getter/setter pair
     * (in either order) sharing a name. Each accessor is parsed independently and, when both are
     * present, merged into one combined {@link StructFieldModel}. Returns {@code null} (with an error
     * already emitted) on any conflict.
     */
    private static StructFieldModel parseField(
        ExecutableElement first,
        ExecutableElement second,
        String structName,
        List<String> priorStructNames,
        ProcessingEnvironment env,
        Messager messager
    ) {
        StructFieldModel a = parseAccessor(first, structName, priorStructNames, env, messager);
        StructFieldModel b = second == null ? null : parseAccessor(second, structName, priorStructNames, env, messager);
        if (a == null || (second != null && b == null)) {
            return null;
        }
        return b == null ? a : merge(a, b, structName, second, messager);
    }

    /**
     * Merges two same-named accessors (a getter and a setter) into one combined field. The two must
     * be the same kind of field and agree on type and length. Returns {@code null} (with an error
     * already emitted) on any conflict.
     */
    private static StructFieldModel merge(
        StructFieldModel a,
        StructFieldModel b,
        String structName,
        Element reportElement,
        Messager messager
    ) {
        if (a.getClass() != b.getClass()) {
            messager.printMessage(
                Kind.ERROR,
                "Field '" + a.name() + "' on '" + structName + "' has adjacent methods with different annotation types",
                reportElement
            );
            return null;
        }
        return switch (a) {
            case ScalarFieldModel sa -> {
                ScalarFieldModel sb = (ScalarFieldModel) b;
                if (sa.hasSetter() && sb.hasSetter()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Duplicate setter for field '" + sa.name() + "' on '" + structName + "'",
                        reportElement
                    );
                    yield null;
                }
                if (sa.type() != sb.type()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Getter and setter for field '"
                            + sa.name()
                            + "' on '"
                            + structName
                            + "' have mismatched types: "
                            + (sa.hasGetter() ? "getter" : "setter")
                            + " has '"
                            + sa.type()
                            + "', "
                            + (sb.hasGetter() ? "getter" : "setter")
                            + " has '"
                            + sb.type()
                            + "'",
                        reportElement
                    );
                    yield null;
                }
                yield new ScalarFieldModel(sa.name(), sa.type(), sa.hasGetter() || sb.hasGetter(), sa.hasSetter() || sb.hasSetter());
            }
            case InlineArrayFieldModel ia -> {
                InlineArrayFieldModel ib = (InlineArrayFieldModel) b;
                if (ia.hasSetter() && ib.hasSetter()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Duplicate @InlineArrayField setter for field '" + ia.name() + "' on '" + structName + "'",
                        reportElement
                    );
                    yield null;
                }
                if (ia.elementType() != ib.elementType()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@InlineArrayField getter and setter for field '"
                            + ia.name()
                            + "' on '"
                            + structName
                            + "' have mismatched element types: '"
                            + ia.elementType()
                            + "' vs '"
                            + ib.elementType()
                            + "'",
                        reportElement
                    );
                    yield null;
                }
                if (ia.length() != ib.length()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@InlineArrayField getter and setter for field '"
                            + ia.name()
                            + "' on '"
                            + structName
                            + "' have mismatched lengths: "
                            + ia.length()
                            + " vs "
                            + ib.length(),
                        reportElement
                    );
                    yield null;
                }
                yield new InlineArrayFieldModel(
                    ia.name(),
                    ia.elementType(),
                    ia.length(),
                    ia.hasGetter() || ib.hasGetter(),
                    ia.hasSetter() || ib.hasSetter()
                );
            }
            case InlineStringFieldModel is -> {
                InlineStringFieldModel isb = (InlineStringFieldModel) b;
                if (is.hasSetter() && isb.hasSetter()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Duplicate @InlineStringField setter for field '" + is.name() + "' on '" + structName + "'",
                        reportElement
                    );
                    yield null;
                }
                if (is.length() != isb.length()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@InlineStringField getter and setter for field '"
                            + is.name()
                            + "' on '"
                            + structName
                            + "' have mismatched lengths: "
                            + is.length()
                            + " vs "
                            + isb.length(),
                        reportElement
                    );
                    yield null;
                }
                yield new InlineStringFieldModel(
                    is.name(),
                    is.length(),
                    is.hasGetter() || isb.hasGetter(),
                    is.hasSetter() || isb.hasSetter()
                );
            }
            case ArrayFieldModel arr -> {
                messager.printMessage(Kind.ERROR, "Duplicate field name '" + arr.name() + "' on '" + structName + "'", reportElement);
                yield null;
            }
        };
    }

    // --- Interface accessor parsing ---

    /**
     * Parses a single abstract method on a {@code @StructSpecification} interface into a single-sided
     * {@link StructFieldModel} shape. Recognises {@code @ArrayField} indexed accessors,
     * {@code @InlineArrayField}/{@code @InlineStringField} fixed-size accessors, and plain scalar
     * getters/setters. Returns {@code null} on any error.
     */
    private static StructFieldModel parseAccessor(
        ExecutableElement method,
        String enclosingStructSimpleName,
        List<String> priorStructNames,
        ProcessingEnvironment env,
        Messager messager
    ) {
        String methodName = method.getSimpleName().toString();
        AnnotationMirror arrayFieldMirror = ModelUtil.findAnnotationMirror(method, ARRAY_FIELD_FQN);
        AnnotationMirror inlineArrayMirror = ModelUtil.findAnnotationMirror(method, "org.elasticsearch.foreign.InlineArrayField");
        AnnotationMirror inlineStringMirror = ModelUtil.findAnnotationMirror(method, "org.elasticsearch.foreign.InlineStringField");

        int annotationCount = (arrayFieldMirror != null ? 1 : 0) + (inlineArrayMirror != null ? 1 : 0) + (inlineStringMirror != null
            ? 1
            : 0);
        if (annotationCount > 1) {
            messager.printMessage(
                Kind.ERROR,
                "Method '"
                    + methodName
                    + "' on @StructSpecification interface '"
                    + enclosingStructSimpleName
                    + "' may not have more than one of @ArrayField, @InlineArrayField, @InlineStringField",
                method
            );
            return null;
        }

        if (arrayFieldMirror != null) {
            if (method.getParameters().size() != 1 || method.getParameters().get(0).asType().getKind() != TypeKind.INT) {
                messager.printMessage(Kind.ERROR, "@ArrayField method '" + methodName + "' must take a single int parameter", method);
                return null;
            }
            TypeMirror returnMirror = method.getReturnType();
            if (returnMirror.getKind() != TypeKind.DECLARED) {
                messager.printMessage(
                    Kind.ERROR,
                    "@ArrayField method '" + methodName + "' must return a @StructSpecification record type",
                    method
                );
                return null;
            }
            TypeElement elementTypeElement = (TypeElement) env.getTypeUtils().asElement(returnMirror);
            String elementSimpleName = elementTypeElement.getSimpleName().toString();
            if (priorStructNames.contains(elementSimpleName) == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@ArrayField method '"
                        + methodName
                        + "' element type '"
                        + elementSimpleName
                        + "' must be a @StructSpecification record declared in the same @LibrarySpecification interface",
                    method,
                    arrayFieldMirror
                );
                return null;
            }
            String lengthField = ModelUtil.annotationStringValue(arrayFieldMirror, "lengthField");
            if (lengthField == null || lengthField.isEmpty()) {
                messager.printMessage(Kind.ERROR, "@ArrayField on '" + methodName + "' requires lengthField", method, arrayFieldMirror);
                return null;
            }
            return new ArrayFieldModel(methodName, elementSimpleName, lengthField);
        }

        if (inlineArrayMirror != null) {
            return buildInlineArrayField(method, methodName, enclosingStructSimpleName, inlineArrayMirror, messager);
        }

        if (inlineStringMirror != null) {
            return buildInlineStringField(method, methodName, enclosingStructSimpleName, inlineStringMirror, messager);
        }

        NativeType returnType = ModelUtil.classifyType(method.getReturnType());
        if (returnType == NativeType.VOID) {
            if (method.getParameters().size() != 1) {
                messager.printMessage(
                    Kind.ERROR,
                    "Void-return method '"
                        + methodName
                        + "' on @StructSpecification interface '"
                        + enclosingStructSimpleName
                        + "' must have exactly one parameter (setter) but has "
                        + method.getParameters().size(),
                    method
                );
                return null;
            }
            NativeType paramType = ModelUtil.classifyType(method.getParameters().get(0).asType());
            if (paramType == null
                || paramType == NativeType.VOID
                || paramType == NativeType.STRING
                || paramType == NativeType.ADDRESSABLE) {
                messager.printMessage(
                    Kind.ERROR,
                    "Setter method '"
                        + methodName
                        + "' on @StructSpecification interface '"
                        + enclosingStructSimpleName
                        + "' has unsupported parameter type '"
                        + method.getParameters().get(0).asType()
                        + "'",
                    method
                );
                return null;
            }
            return new ScalarFieldModel(methodName, paramType, false, true);
        }

        if (returnType == null || returnType == NativeType.STRING || returnType == NativeType.ADDRESSABLE) {
            messager.printMessage(
                Kind.ERROR,
                "Unsupported field type '"
                    + method.getReturnType()
                    + "' on method '"
                    + methodName
                    + "' of @StructSpecification interface '"
                    + enclosingStructSimpleName
                    + "'",
                method
            );
            return null;
        }
        if (method.getParameters().isEmpty() == false) {
            messager.printMessage(Kind.ERROR, "Scalar field getter '" + methodName + "' must take no parameters", method);
            return null;
        }
        return new ScalarFieldModel(methodName, returnType, true, false);
    }

    private static StructFieldModel buildInlineArrayField(
        ExecutableElement method,
        String methodName,
        String enclosingStructSimpleName,
        AnnotationMirror inlineArrayMirror,
        Messager messager
    ) {
        Integer length = ModelUtil.annotationIntValue(inlineArrayMirror, "length");
        if (length == null || length <= 0) {
            messager.printMessage(
                Kind.ERROR,
                "@InlineArrayField on '" + methodName + "' in '" + enclosingStructSimpleName + "' requires a positive length",
                method,
                inlineArrayMirror
            );
            return null;
        }

        TypeMirror returnMirror = method.getReturnType();
        boolean isVoid = returnMirror.getKind() == TypeKind.VOID;
        int paramCount = method.getParameters().size();

        if (isVoid) {
            if (paramCount != 2 || method.getParameters().get(0).asType().getKind() != TypeKind.INT) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineArrayField setter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' must have signature void fieldName(int index, <primitive> value)",
                    method
                );
                return null;
            }
            NativeType valueType = ModelUtil.classifyType(method.getParameters().get(1).asType());
            if (valueType == null
                || valueType == NativeType.VOID
                || valueType == NativeType.STRING
                || valueType == NativeType.ADDRESSABLE
                || valueType == NativeType.ADDRESS) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineArrayField setter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' has unsupported value type '"
                        + method.getParameters().get(1).asType()
                        + "'",
                    method
                );
                return null;
            }
            return new InlineArrayFieldModel(methodName, valueType, length, false, true);
        } else {
            if (paramCount != 1 || method.getParameters().get(0).asType().getKind() != TypeKind.INT) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineArrayField getter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' must have signature <primitive> fieldName(int index)",
                    method
                );
                return null;
            }
            NativeType elementType = ModelUtil.classifyType(returnMirror);
            if (elementType == null
                || elementType == NativeType.VOID
                || elementType == NativeType.STRING
                || elementType == NativeType.ADDRESSABLE
                || elementType == NativeType.ADDRESS) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineArrayField getter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' must return a primitive type, got '"
                        + returnMirror
                        + "'",
                    method
                );
                return null;
            }
            return new InlineArrayFieldModel(methodName, elementType, length, true, false);
        }
    }

    private static StructFieldModel buildInlineStringField(
        ExecutableElement method,
        String methodName,
        String enclosingStructSimpleName,
        AnnotationMirror inlineStringMirror,
        Messager messager
    ) {
        Integer length = ModelUtil.annotationIntValue(inlineStringMirror, "length");
        if (length == null || length <= 0) {
            messager.printMessage(
                Kind.ERROR,
                "@InlineStringField on '" + methodName + "' in '" + enclosingStructSimpleName + "' requires a positive length",
                method,
                inlineStringMirror
            );
            return null;
        }

        TypeMirror returnMirror = method.getReturnType();
        boolean isVoid = returnMirror.getKind() == TypeKind.VOID;
        int paramCount = method.getParameters().size();

        if (isVoid) {
            if (paramCount != 1) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineStringField setter '" + methodName + "' in '" + enclosingStructSimpleName + "' must have exactly one parameter",
                    method
                );
                return null;
            }
            NativeType paramType = ModelUtil.classifyType(method.getParameters().get(0).asType());
            if (paramType != NativeType.STRING) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineStringField setter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' parameter must be String, got '"
                        + method.getParameters().get(0).asType()
                        + "'",
                    method
                );
                return null;
            }
            return new InlineStringFieldModel(methodName, length, false, true);
        } else {
            NativeType returnType = ModelUtil.classifyType(returnMirror);
            if (returnType != NativeType.STRING) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineStringField getter '"
                        + methodName
                        + "' in '"
                        + enclosingStructSimpleName
                        + "' must return String, got '"
                        + returnMirror
                        + "'",
                    method
                );
                return null;
            }
            if (paramCount != 0) {
                messager.printMessage(
                    Kind.ERROR,
                    "@InlineStringField getter '" + methodName + "' in '" + enclosingStructSimpleName + "' must take no parameters",
                    method
                );
                return null;
            }
            return new InlineStringFieldModel(methodName, length, true, false);
        }
    }
}
