/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.processor.model.StructFieldModel;
import org.elasticsearch.foreign.processor.model.StructLayoutModel;

import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.classfile.instruction.SwitchCase;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_StructLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_structLayout;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.emitStructLayoutArray;

/**
 * Shared generation of the {@code LAYOUT} static field for a struct's {@code $Impl}/{@code $Pack}
 * class. When the struct resolves to a single layout across every supported platform (the common
 * case), a single {@code MemoryLayout.structLayout(...)} is stored. When it resolves to several, a
 * {@code <clinit>} switch on {@code Platform.current().ordinal()} selects the running platform's
 * layout; the distinct layouts are already grouped in the model as {@link StructLayoutModel}s.
 */
final class PerPlatformClinit {

    private static final ClassDesc CD_Platform = ClassDesc.of("org.elasticsearch.foreign.Platform");
    private static final ClassDesc CD_AssertionError = ClassDesc.of("java.lang.AssertionError");
    private static final ClassDesc CD_Object = ClassDesc.of("java.lang.Object");
    private static final MethodTypeDesc MTD_Platform_current = MethodTypeDesc.of(CD_Platform);
    private static final MethodTypeDesc MTD_ordinal = MethodTypeDesc.of(ClassDesc.ofDescriptor("I"));

    private PerPlatformClinit() {}

    /** {@code true} when the struct resolves to more than one distinct layout across platforms. */
    static boolean isPerPlatform(List<StructLayoutModel> layouts) {
        return layouts.size() > 1;
    }

    /**
     * Emits the code that initializes the {@code LAYOUT} field. For a single layout this is a straight
     * {@code structLayout(...)} store; for several it is a {@code Platform.current().ordinal()} switch,
     * one arm per {@link StructLayoutModel}. On return, {@code LAYOUT} is initialized and control
     * continues in normal flow (the switch has re-joined). Uses local slot 0 (a {@code <clinit>} has
     * no {@code this}).
     */
    static void emitLayoutInit(CodeBuilder clinit, ClassDesc structDesc, List<StructLayoutModel> layouts, List<StructFieldModel> fields) {
        Map<String, StructFieldModel> fieldsByName = new LinkedHashMap<>();
        for (StructFieldModel field : fields) {
            fieldsByName.put(field.name(), field);
        }
        if (layouts.size() == 1) {
            emitStoreLayout(clinit, structDesc, layouts.get(0), fieldsByName);
            return;
        }

        clinit.invokestatic(CD_Platform, "current", MTD_Platform_current, false);
        clinit.astore(0);
        clinit.aload(0);
        clinit.invokevirtual(CD_Platform, "ordinal", MTD_ordinal);

        var afterSwitch = clinit.newLabel();
        var defaultLabel = clinit.newLabel();

        List<SwitchCase> cases = new ArrayList<>();
        List<Label> layoutLabels = new ArrayList<>();
        for (StructLayoutModel layout : layouts) {
            var label = clinit.newLabel();
            layoutLabels.add(label);
            for (String platform : layout.platforms()) {
                cases.add(SwitchCase.of(Platform.valueOf(platform).ordinal(), label));
            }
        }
        cases.sort(Comparator.comparingInt(SwitchCase::caseValue));
        clinit.lookupswitch(defaultLabel, cases);

        for (int i = 0; i < layouts.size(); i++) {
            clinit.labelBinding(layoutLabels.get(i));
            emitStoreLayout(clinit, structDesc, layouts.get(i), fieldsByName);
            clinit.goto_(afterSwitch);
        }

        // Default: throw new AssertionError(platform) for an unsupported running platform.
        clinit.labelBinding(defaultLabel);
        clinit.new_(CD_AssertionError);
        clinit.dup();
        clinit.aload(0);
        clinit.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Object));
        clinit.athrow();

        clinit.labelBinding(afterSwitch);
    }

    private static void emitStoreLayout(
        CodeBuilder clinit,
        ClassDesc structDesc,
        StructLayoutModel layout,
        Map<String, StructFieldModel> fieldsByName
    ) {
        emitStructLayoutArray(clinit, layout.layout(), fieldsByName);
        clinit.invokestatic(CD_MemoryLayout, "structLayout", MTD_structLayout, true);
        clinit.putstatic(structDesc, "LAYOUT", CD_StructLayout);
    }
}
