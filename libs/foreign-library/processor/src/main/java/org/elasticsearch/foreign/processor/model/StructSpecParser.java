/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.Platform;

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
    private static final String STRUCT_SIZE_FQN = "org.elasticsearch.foreign.StructSize";
    private static final String STRUCT_SIZE_LIST_FQN = "org.elasticsearch.foreign.StructSize.List";
    private static final String SIZEOF_FQN = "org.elasticsearch.foreign.Sizeof";

    /**
     * Builds the {@link StructModel} for a {@code @StructSpecification} record. Record components are
     * scalar getters laid out in declaration order; {@code @Offset} on a component and
     * {@code @StructSize} on the record are honoured exactly as for interfaces. Emits errors for
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
                || fieldType == NativeType.ADDRESSABLE
                || fieldType == NativeType.UPCALL) {
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
            error |= placeNewField(layout, field, isSparse, offsetMirrors, supportedPlatforms, typeSimpleName, component, messager);
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
        return buildStructModel(typeSimpleName, layout, byteSizes, supportedPlatforms, /* isRecord */ true, isSparse, null);
    }

    /**
     * Builds the {@link StructModel} for a {@code @StructSpecification} interface. Walks the abstract
     * methods once, merging a getter/setter pair (which must be declared adjacently) into a single
     * field, validating {@code @ArrayField} length references, and placing every field at its resolved
     * absolute offset for each supported platform. Returns {@code null} on any error.
     *
     * @param unavailableOn the enclosing {@code @LibrarySpecification}'s {@code unavailableOn} platform names,
     *        used to reject {@code @InlineStringField(wide = true)} fields on libraries that are unavailable
     *        on Windows
     */
    static StructModel fromInterface(
        TypeElement typeElement,
        List<String> priorStructNames,
        Set<String> supportedPlatforms,
        List<String> unavailableOn,
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
        // setter) or two adjacent ones sharing a name (a getter/setter pair). @Sizeof methods are
        // pulled out separately below — they contribute no field.
        List<ExecutableElement> methods = new ArrayList<>();
        String sizeofMethodName = null;
        for (var enclosedMember : typeElement.getEnclosedElements()) {
            if (enclosedMember.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) enclosedMember;
            var mods = method.getModifiers();
            if (mods.contains(Modifier.DEFAULT) || mods.contains(Modifier.STATIC)) {
                continue;
            }
            AnnotationMirror sizeofMirror = ModelUtil.findAnnotationMirror(method, SIZEOF_FQN);
            if (sizeofMirror != null) {
                if (sizeofMethodName != null) {
                    messager.printMessage(
                        Kind.ERROR,
                        "Duplicate @Sizeof method on '"
                            + typeSimpleName
                            + "': '"
                            + sizeofMethodName
                            + "' and '"
                            + method.getSimpleName()
                            + "'",
                        method
                    );
                    error = true;
                    continue;
                }
                if (validateSizeofMethod(method, typeSimpleName, messager) == false) {
                    error = true;
                    continue;
                }
                sizeofMethodName = method.getSimpleName().toString();
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
            StructFieldModel field = parseField(first, second, typeSimpleName, priorStructNames, unavailableOn, env, messager);
            if (field == null) {
                error = true;
                continue;
            }
            layout.markSeen(name);
            if (field instanceof ScalarFieldModel) {
                scalarFieldNames.add(name);
            }

            // @Offset is only permitted on the first-declared accessor.
            List<AnnotationMirror> offsetMirrors = ModelUtil.collectRepeatableAnnotations(first, OFFSET_FQN, OFFSET_LIST_FQN);
            if (second != null && ModelUtil.collectRepeatableAnnotations(second, OFFSET_FQN, OFFSET_LIST_FQN).isEmpty() == false) {
                messager.printMessage(
                    Kind.ERROR,
                    "@Offset on '" + name + "' in '" + typeSimpleName + "' must be only on the first-declared accessor of the field",
                    second
                );
                error = true;
            }

            error |= placeNewField(layout, field, isSparse, offsetMirrors, supportedPlatforms, typeSimpleName, first, messager);
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
        return buildStructModel(typeSimpleName, layout, byteSizes, supportedPlatforms, /* isRecord */ false, isSparse, sizeofMethodName);
    }

    /**
     * Validates that a {@code @Sizeof} method has the required {@code int name()} shape: {@code int}
     * return type, zero parameters. Returns {@code false} (with an error already emitted) if not.
     */
    private static boolean validateSizeofMethod(ExecutableElement method, String typeSimpleName, Messager messager) {
        boolean valid = true;
        if (method.getReturnType().getKind() != TypeKind.INT) {
            messager.printMessage(
                Kind.ERROR,
                "@Sizeof method '" + method.getSimpleName() + "' on '" + typeSimpleName + "' must return int",
                method
            );
            valid = false;
        }
        if (method.getParameters().isEmpty() == false) {
            messager.printMessage(
                Kind.ERROR,
                "@Sizeof method '" + method.getSimpleName() + "' on '" + typeSimpleName + "' must take no parameters",
                method
            );
            valid = false;
        }
        return valid;
    }

    // --- Layout accumulation ---

    /**
     * Accumulates the field shapes (recorded once, identical across platforms) and, in sparse mode,
     * each field's absolute {@code @Offset} per platform plus the running end (used to validate that
     * offsets do not overlap). Dense structs record no per-platform values — the builder derives their
     * natural-aligned layout from the field shapes alone.
     */
    private static final class LayoutBuilder {
        private final List<StructFieldModel> fields = new ArrayList<>();
        private final Map<String, List<Long>> values = new LinkedHashMap<>();
        private final Map<String, Long> cursor = new LinkedHashMap<>();
        private final Set<String> seenNames = new LinkedHashSet<>();

        LayoutBuilder(Set<String> platforms) {
            for (String p : platforms) {
                cursor.put(p, 0L);
                values.put(p, new ArrayList<>());
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

        void advanceCursor(String platform, long newCursor) {
            cursor.put(platform, newCursor);
        }

        /** Records a field's shape once (shared across platforms). */
        void addField(StructFieldModel field) {
            fields.add(field);
        }

        /** Appends a field's per-platform layout value (sparse offset, or dense padding / {@code null}). */
        void addValue(String platform, Long value) {
            values.get(platform).add(value);
        }

        /** Field shapes in declaration order — identical across platforms. */
        List<StructFieldModel> fields() {
            return fields;
        }

        /** Per-field layout values for one platform, index-aligned with {@link #fields()}. */
        List<Long> valuesFor(String platform) {
            return values.get(platform);
        }
    }

    /**
     * Places a field on every platform: records its shape once and, in sparse mode, its absolute
     * {@code @Offset} per platform (validating it does not overlap the previous field). In dense mode
     * fields are laid out with natural alignment by the builder, so only the disallowed {@code @Offset}
     * is checked here. Returns {@code true} if any error was emitted.
     */
    private static boolean placeNewField(
        LayoutBuilder layout,
        StructFieldModel field,
        boolean isSparse,
        List<AnnotationMirror> offsetMirrors,
        Set<String> supportedPlatforms,
        String typeSimpleName,
        Element reportElement,
        Messager messager
    ) {
        String name = field.name();
        boolean error = false;
        layout.addField(field);

        if (isSparse) {
            long size = FieldLayouts.memberLayout(field).byteSize();
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
                    layout.addValue(p, cursor);
                    layout.advanceCursor(p, cursor + size);
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
                layout.addValue(p, offset);
                layout.advanceCursor(p, offset + size);
            }
        } else if (offsetMirrors.isEmpty() == false) {
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

        return error;
    }

    /**
     * Resolves the total struct size per platform in sparse mode: the resolved {@code @StructSize},
     * validated to be no smaller than the struct's content (the running end tracked during placement).
     * Dense structs carry no explicit size — the dense layout builder derives it — so this returns an
     * empty map. Returns {@code null} if any error was emitted.
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
        if (isSparse == false) {
            return Map.of();
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
        Map<String, Long> byteSizes = new LinkedHashMap<>();
        for (String p : supportedPlatforms) {
            long total = sizes.get(p);
            long end = layout.cursor(p);
            if (total < end) {
                messager.printMessage(
                    Kind.ERROR,
                    "@StructSize " + total + " in '" + typeSimpleName + "' is smaller than the end of the last field at " + end,
                    reportElement
                );
                error = true;
            }
            byteSizes.put(p, total);
        }
        return error ? null : byteSizes;
    }

    /**
     * Builds the struct's {@link StructLayoutModel}s. A dense struct has one natural-aligned layout
     * shared by every supported platform. A sparse struct is built per platform from its resolved
     * offsets, then platforms with an identical layout are collapsed into one entry — in {@code
     * Platform} ordinal order, since {@code supportedPlatforms} is iterated in that order.
     */
    private static StructModel buildStructModel(
        String typeSimpleName,
        LayoutBuilder layout,
        Map<String, Long> byteSizes,
        Set<String> supportedPlatforms,
        boolean isRecord,
        boolean isSparse,
        String sizeofMethodName
    ) {
        List<StructFieldModel> fields = List.copyOf(layout.fields());
        List<StructLayoutModel> layouts;
        if (isSparse) {
            Map<MemoryLayout, List<String>> platformsByLayout = new LinkedHashMap<>();
            for (String p : supportedPlatforms) {
                MemoryLayout memoryLayout = FieldLayouts.sparseStructLayout(fields, layout.valuesFor(p), byteSizes.get(p));
                platformsByLayout.computeIfAbsent(memoryLayout, k -> new ArrayList<>()).add(p);
            }
            layouts = new ArrayList<>();
            for (var entry : platformsByLayout.entrySet()) {
                layouts.add(new StructLayoutModel(List.copyOf(entry.getValue()), entry.getKey()));
            }
        } else {
            layouts = List.of(new StructLayoutModel(List.copyOf(supportedPlatforms), FieldLayouts.denseStructLayout(fields)));
        }
        return isRecord
            ? new StructRecordModel(typeSimpleName, fields, layouts)
            : new StructInterfaceModel(typeSimpleName, fields, layouts, sizeofMethodName);
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
     * Resolves a list of repeated layout annotation mirrors (e.g. {@code @Offset}/{@code @StructSize})
     * to a value per supported platform. A bare annotation (empty {@code platforms}) is the fallback
     * for any platform without a specific entry; per-platform entries override it. Validates that at
     * most one such fallback is present, no platform is covered twice, and every supported platform
     * resolves. Returns {@code null} on any error (already emitted).
     */
    private static Map<String, Integer> resolvePerPlatform(
        List<AnnotationMirror> mirrors,
        Set<String> supportedPlatforms,
        String annotationName,
        Element reportElement,
        Messager messager
    ) {
        Integer fallback = null;
        Map<String, Integer> perPlatform = new LinkedHashMap<>();
        boolean error = false;

        for (AnnotationMirror mirror : mirrors) {
            Integer value = ModelUtil.annotationIntValue(mirror, "value");
            Set<String> platforms = ModelUtil.extractPlatforms(mirror);
            if (platforms.isEmpty()) {
                if (fallback != null) {
                    messager.printMessage(
                        Kind.ERROR,
                        "More than one platform-independent @" + annotationName + " on '" + reportElement.getSimpleName() + "'",
                        reportElement
                    );
                    error = true;
                } else {
                    fallback = value;
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
            } else if (fallback != null) {
                resolved.put(platform, fallback);
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
        List<String> unavailableOn,
        ProcessingEnvironment env,
        Messager messager
    ) {
        StructFieldModel a = parseAccessor(first, structName, priorStructNames, unavailableOn, env, messager);
        StructFieldModel b = second == null ? null : parseAccessor(second, structName, priorStructNames, unavailableOn, env, messager);
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
                if (is.wide() != isb.wide()) {
                    messager.printMessage(
                        Kind.ERROR,
                        "@InlineStringField getter and setter for '" + is.name() + "' disagree on wide=; both must be the same",
                        reportElement
                    );
                    yield null;
                }
                yield new InlineStringFieldModel(
                    is.name(),
                    is.length(),
                    is.wide(),
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
        List<String> unavailableOn,
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
            return buildInlineStringField(method, methodName, enclosingStructSimpleName, inlineStringMirror, unavailableOn, messager);
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
                || paramType == NativeType.ADDRESSABLE
                || paramType == NativeType.UPCALL) {
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

        if (returnType == null
            || returnType == NativeType.STRING
            || returnType == NativeType.ADDRESSABLE
            || returnType == NativeType.UPCALL) {
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
                || valueType == NativeType.UPCALL
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
                || elementType == NativeType.UPCALL
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
        List<String> unavailableOn,
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

        boolean wide = ModelUtil.annotationBooleanValue(inlineStringMirror, "wide", false);
        if (wide && length % 2 != 0) {
            messager.printMessage(
                Kind.ERROR,
                "@InlineStringField(wide = true) requires an even length in bytes; got " + length + " on '" + methodName + "'",
                method,
                inlineStringMirror
            );
            return null;
        }
        if (wide && unavailableOn.contains(Platform.WINDOWS_X64.name())) {
            messager.printMessage(
                Kind.ERROR,
                "@InlineStringField(wide = true) on '"
                    + methodName
                    + "' is invalid: enclosing @LibrarySpecification lists WINDOWS_X64 in unavailableOn",
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
            return new InlineStringFieldModel(methodName, length, wide, false, true);
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
            return new InlineStringFieldModel(methodName, length, wide, true, false);
        }
    }
}
