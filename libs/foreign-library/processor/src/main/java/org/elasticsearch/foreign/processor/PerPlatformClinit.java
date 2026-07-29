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
import org.elasticsearch.foreign.processor.model.StructModel;

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
import static org.elasticsearch.foreign.processor.StructLayoutUtil.deriveLayout;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.emitStructLayoutArray;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.trailingPadding;

/**
 * Shared generation of the {@code LAYOUT} static field for a struct's {@code $Impl}/{@code $Pack}
 * class. When a struct's layout is identical across every supported platform, a single
 * {@code MemoryLayout.structLayout(...)} is emitted. When it differs, a {@code <clinit>} switch on
 * {@code Platform.current().ordinal()} selects the layout for the running platform, with platforms
 * sharing an identical layout collapsed into one case block.
 */
final class PerPlatformClinit {

    private static final ClassDesc CD_Platform = ClassDesc.of("org.elasticsearch.foreign.Platform");
    private static final ClassDesc CD_AssertionError = ClassDesc.of("java.lang.AssertionError");
    private static final ClassDesc CD_Object = ClassDesc.of("java.lang.Object");
    private static final MethodTypeDesc MTD_Platform_current = MethodTypeDesc.of(CD_Platform);
    private static final MethodTypeDesc MTD_ordinal = MethodTypeDesc.of(ClassDesc.ofDescriptor("I"));

    private PerPlatformClinit() {}

    /** A distinct concrete layout and the platforms that resolve to it. */
    record Group(StructModel model, List<String> platforms) {}

    /**
     * Groups a struct's per-platform models by layout equality, preserving first-seen platform order.
     * Two platforms share a group when their {@link StructModel} instances are {@code equals} — i.e.
     * identical field shape and offsets and total size.
     */
    static List<Group> group(Map<String, StructModel> byPlatform) {
        Map<StructModel, List<String>> byModel = new LinkedHashMap<>();
        for (var entry : byPlatform.entrySet()) {
            byModel.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(entry.getKey());
        }
        List<Group> groups = new ArrayList<>();
        for (var entry : byModel.entrySet()) {
            groups.add(new Group(entry.getKey(), entry.getValue()));
        }
        return groups;
    }

    /** {@code true} when the struct has more than one distinct layout across platforms. */
    static boolean isPerPlatform(List<Group> groups) {
        return groups.size() > 1;
    }

    /**
     * Emits the code that initializes the {@code LAYOUT} field. For a single group this is a straight
     * {@code structLayout(...)} store; for multiple groups it is a {@code Platform.current().ordinal()}
     * switch. On return, {@code LAYOUT} is initialized and control continues in normal flow (the
     * per-platform switch has re-joined). Uses local slot 0 (a {@code <clinit>} has no {@code this}).
     */
    static void emitLayoutInit(CodeBuilder clinit, ClassDesc structDesc, List<Group> groups) {
        if (groups.size() == 1) {
            emitStoreLayout(clinit, structDesc, groups.get(0).model());
            return;
        }

        clinit.invokestatic(CD_Platform, "current", MTD_Platform_current, false);
        clinit.astore(0);
        clinit.aload(0);
        clinit.invokevirtual(CD_Platform, "ordinal", MTD_ordinal);

        var afterSwitch = clinit.newLabel();
        var defaultLabel = clinit.newLabel();

        List<SwitchCase> cases = new ArrayList<>();
        List<Label> groupLabels = new ArrayList<>();
        for (Group g : groups) {
            var label = clinit.newLabel();
            groupLabels.add(label);
            for (String platform : g.platforms()) {
                cases.add(SwitchCase.of(Platform.valueOf(platform).ordinal(), label));
            }
        }
        cases.sort(Comparator.comparingInt(SwitchCase::caseValue));
        clinit.lookupswitch(defaultLabel, cases);

        for (int i = 0; i < groups.size(); i++) {
            clinit.labelBinding(groupLabels.get(i));
            emitStoreLayout(clinit, structDesc, groups.get(i).model());
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

    private static void emitStoreLayout(CodeBuilder clinit, ClassDesc structDesc, StructModel model) {
        emitStructLayoutArray(clinit, deriveLayout(model.fields()), trailingPadding(model));
        clinit.invokestatic(CD_MemoryLayout, "structLayout", MTD_structLayout, true);
        clinit.putstatic(structDesc, "LAYOUT", CD_StructLayout);
    }
}
