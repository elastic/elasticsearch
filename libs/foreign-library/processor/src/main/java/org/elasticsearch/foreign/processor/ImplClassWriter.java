/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.LinkerHelper;
import org.elasticsearch.foreign.LoaderHelper;
import org.elasticsearch.foreign.processor.model.ArrayFieldModel;
import org.elasticsearch.foreign.processor.model.BoundsCheckModel;
import org.elasticsearch.foreign.processor.model.LibraryModel;
import org.elasticsearch.foreign.processor.model.MethodModel;
import org.elasticsearch.foreign.processor.model.NativeType;
import org.elasticsearch.foreign.processor.model.ScalarFieldModel;
import org.elasticsearch.foreign.processor.model.StructInterfaceModel;
import org.elasticsearch.foreign.processor.model.StructModel;
import org.elasticsearch.foreign.processor.model.StructRecordModel;

import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.processing.Filer;
import javax.lang.model.element.TypeElement;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Addressable;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Arena;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_ArenaAdapter;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayoutArray;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegment;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegmentAdapter;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Object;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_String;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_StructLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_VarHandle;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_boolean;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_ArenaAdapter_allocate;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_Arena_ofAuto;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.primitiveClassDesc;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.slotWidth;
import static org.elasticsearch.foreign.processor.model.LibraryModel.RESOLVER_INTERFACE_FQN;

/**
 * Generates {@code <InterfaceName>$Impl} class files for {@code @LibrarySpecification}-annotated interfaces,
 * plus {@code $Pack} companion classes for {@code @StructSpecification} records and {@code $Impl} classes
 * for {@code @StructSpecification} interfaces.
 *
 * <p>Each generated library {@code $Impl} class:
 * <ul>
 *   <li>is package-private {@code final} with a package-private no-arg constructor</li>
 *   <li>implements the annotated interface</li>
 *   <li>has one {@code private static final MethodHandle} field per {@code @Function} method</li>
 *   <li>initializes those fields in {@code <clinit>}</li>
 *   <li>implements each interface method by calling {@code MethodHandle.invokeExact}</li>
 *   <li>implements each {@code @StructFactory} method by constructing the appropriate struct</li>
 * </ul>
 */
class ImplClassWriter {

    private static final ClassDesc CD_MethodHandle = ClassDesc.of("java.lang.invoke.MethodHandle");
    private static final ClassDesc CD_MethodHandles = ClassDesc.of("java.lang.invoke.MethodHandles");
    private static final ClassDesc CD_Lookup = ClassDesc.of("java.lang.invoke.MethodHandles$Lookup");
    private static final ClassDesc CD_FunctionDescriptor = ClassDesc.of("java.lang.foreign.FunctionDescriptor");
    private static final ClassDesc CD_LinkerOption = ClassDesc.of("java.lang.foreign.Linker$Option");
    private static final ClassDesc CD_LinkerOptionArray = ClassDesc.ofDescriptor("[Ljava/lang/foreign/Linker$Option;");
    private static final ClassDesc CD_AssertionError = ClassDesc.of("java.lang.AssertionError");
    private static final ClassDesc CD_Class = ClassDesc.of("java.lang.Class");
    private static final ClassDesc CD_Linker = ClassDesc.of("java.lang.foreign.Linker");
    private static final ClassDesc CD_SymbolLookup = ClassDesc.of("java.lang.foreign.SymbolLookup");
    private static final ClassDesc CD_LinkerHelper = ClassDesc.of(LinkerHelper.class.getName());
    private static final ClassDesc CD_LinkerAdapter = ClassDesc.of("org.elasticsearch.foreign.adapter.LinkerAdapter"); // not a dependency
    private static final ClassDesc CD_LoaderHelper = ClassDesc.of(LoaderHelper.class.getName());
    private static final ClassDesc CD_Objects = ClassDesc.of("java.util.Objects");
    private static final ClassDesc CD_IllegalArgumentException = ClassDesc.of("java.lang.IllegalArgumentException");

    /**
     * Name of the synthetic field holding the negated result of {@code Class.desiredAssertionStatus()},
     * mirroring javac's own convention for source-level {@code assert} statements.
     */
    private static final String ASSERTIONS_DISABLED_FIELD = "$assertionsDisabled";

    private static final MethodTypeDesc MTD_byteSize = MethodTypeDesc.of(CD_long);
    private static final MethodTypeDesc MTD_checkFromIndexSize = MethodTypeDesc.of(CD_long, CD_long, CD_long, CD_long);
    private static final MethodTypeDesc MTD_desiredAssertionStatus = MethodTypeDesc.of(CD_boolean);
    private static final MethodTypeDesc MTD_FunctionDescriptor_ofVoid = MethodTypeDesc.of(CD_FunctionDescriptor, CD_MemoryLayoutArray);
    private static final MethodTypeDesc MTD_FunctionDescriptor_of = MethodTypeDesc.of(
        CD_FunctionDescriptor,
        CD_MemoryLayout,
        CD_MemoryLayoutArray
    );
    private static final MethodTypeDesc MTD_downcallHandle_byAddress = MethodTypeDesc.of(
        CD_MethodHandle,
        CD_MemorySegment,
        CD_FunctionDescriptor,
        CD_LinkerOptionArray
    );
    private static final MethodTypeDesc MTD_nativeLinker = MethodTypeDesc.of(CD_Linker);
    private static final MethodTypeDesc MTD_defaultLookup = MethodTypeDesc.of(CD_SymbolLookup);
    private static final MethodTypeDesc MTD_resolve = MethodTypeDesc.of(CD_MemorySegment, CD_String, CD_SymbolLookup);
    private static final MethodTypeDesc MTD_adaptCritical = MethodTypeDesc.of(
        CD_MethodHandle,
        CD_Lookup,
        CD_MethodHandle,
        CD_Class,
        CD_String
    );
    private static final MethodTypeDesc MTD_MemorySegmentAdapter_getString = MethodTypeDesc.of(CD_String, CD_MemorySegment, CD_long);
    private static final MethodTypeDesc MTD_Arena_ofConfined = MethodTypeDesc.of(CD_Arena);
    private static final MethodTypeDesc MTD_Arena_close = MethodTypeDesc.of(CD_void);
    private static final MethodTypeDesc MTD_MemorySegmentAdapter_allocateString = MethodTypeDesc.of(CD_MemorySegment, CD_Arena, CD_String);
    private static final MethodTypeDesc MTD_critical = MethodTypeDesc.of(CD_LinkerOptionArray);

    private final Filer filer;
    private final int classFileVersion;
    private final StructPackWriter packWriter;
    private final StructImplWriter structImplWriter;

    ImplClassWriter(Filer filer, int classFileVersion) {
        this.filer = filer;
        this.classFileVersion = classFileVersion;
        this.packWriter = new StructPackWriter(filer, classFileVersion);
        this.structImplWriter = new StructImplWriter(filer, classFileVersion);
    }

    /** Generates and writes the {@code $Impl} class for the given library model, plus struct companion classes. */
    void generate(LibraryModel model, TypeElement sourceElement) throws Exception {
        // Generate $Pack for record structs and $Impl for interface structs
        for (StructModel struct : model.structs()) {
            switch (struct) {
                case StructRecordModel r -> packWriter.generate(model, r, sourceElement);
                case StructInterfaceModel i -> structImplWriter.generate(model, i, sourceElement);
            }
        }

        ClassDesc generatedDesc = ClassDesc.of(model.implQualifiedName());
        ClassDesc superDesc = ClassDesc.of(model.qualifiedName());
        List<MethodModel> nativeMethods = model.methods();

        // Collect only non-struct-factory methods (methods with @Function)
        List<MethodModel> functionMethods = nativeMethods.stream().filter(m -> m.isStructFactory() == false).toList();

        // Disambiguate MethodHandle field names: single-name groups keep <name>$mh;
        // collision groups (same Java method name) use <name>$0$mh, <name>$1$mh, etc.
        Map<MethodModel, String> fieldNames = computeFieldNames(functionMethods);

        byte[] classBytes = ClassFile.of().build(generatedDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            if (model.isAbstractClass()) {
                cb.withSuperclass(superDesc);
            } else {
                cb.withSuperclass(CD_Object);
                cb.withInterfaceSymbols(superDesc);
            }

            // MethodHandle fields: one per @Function method
            for (var nm : functionMethods) {
                cb.withField(
                    fieldNames.get(nm),
                    CD_MethodHandle,
                    fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL)
                );
            }

            // Backs the `assert`s emitted for @VectorSegment/@MatrixSegment(aligned = true) checks.
            cb.withField(
                ASSERTIONS_DISABLED_FIELD,
                CD_boolean,
                fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL)
            );

            // <clinit>: load the library, resolve the assertions flag, and initialize each MethodHandle field
            cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
                if (model.libraryName().isEmpty() == false) {
                    emitLoadLibrary(clinit, model.libraryName());
                }
                emitAssertionsDisabledInit(clinit, generatedDesc);
                for (var nm : functionMethods) {
                    emitMhFieldInit(clinit, generatedDesc, nm, model.symbolResolverClassName(), fieldNames.get(nm));
                }
                clinit.return_();
            });

            // <init>: package-private no-arg constructor
            cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), 0, init -> {
                init.aload(0);
                init.invokespecial(model.isAbstractClass() ? superDesc : CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                init.return_();
            });

            // @Function method implementations
            for (var nm : functionMethods) {
                emitNativeFunctionMethod(cb, generatedDesc, nm, fieldNames.get(nm));
            }

            // @StructFactory method implementations
            for (var nm : nativeMethods) {
                if (nm.isStructFactory()) {
                    emitStructFactoryMethod(cb, model, nm);
                }
            }
        });

        try (var os = filer.createClassFile(model.implQualifiedName(), sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    /**
     * Computes the {@code MethodHandle} field name for each {@code @Function} method. Methods
     * whose Java name is unique within {@code functionMethods} keep the existing {@code <name>$mh}
     * form. Methods that share a Java name with at least one other method are disambiguated by
     * declaration-order ordinal: {@code <name>$0$mh}, {@code <name>$1$mh}, etc.
     */
    private static Map<MethodModel, String> computeFieldNames(List<MethodModel> functionMethods) {
        // Count how many methods share each Java name to detect collisions.
        Map<String, Integer> nameCount = new HashMap<>();
        for (var nm : functionMethods) {
            nameCount.merge(nm.methodName(), 1, Integer::sum);
        }

        Map<MethodModel, String> fieldNames = new IdentityHashMap<>();
        Map<String, Integer> nameOrdinal = new HashMap<>();
        for (var nm : functionMethods) {
            String fieldName;
            if (nameCount.get(nm.methodName()) == 1) {
                fieldName = nm.methodName() + "$mh";
            } else {
                int ordinal = nameOrdinal.getOrDefault(nm.methodName(), 0);
                nameOrdinal.put(nm.methodName(), ordinal + 1);
                fieldName = nm.methodName() + "$" + ordinal + "$mh";
            }
            fieldNames.put(nm, fieldName);
        }
        return fieldNames;
    }

    // -------------------------------------------------------------------------
    // <clinit> helpers
    // -------------------------------------------------------------------------

    private static void emitLoadLibrary(CodeBuilder cb, String libName) {
        cb.ldc(libName);
        cb.invokestatic(CD_LoaderHelper, "loadLibrary", MethodTypeDesc.of(CD_void, CD_String));
    }

    /**
     * Initializes {@link #ASSERTIONS_DISABLED_FIELD} exactly as javac does for a source-level
     * {@code assert} statement — {@code $assertionsDisabled = !GeneratedClass.class.desiredAssertionStatus();}
     */
    private static void emitAssertionsDisabledInit(CodeBuilder cb, ClassDesc generatedDesc) {
        cb.ldc(generatedDesc);
        cb.invokevirtual(CD_Class, "desiredAssertionStatus", MTD_desiredAssertionStatus);
        var enabled = cb.newLabel();
        var stored = cb.newLabel();
        cb.ifne(enabled); // desiredAssertionStatus() true -> assertions enabled
        cb.iconst_1(); // disabled = true
        cb.goto_(stored);
        cb.labelBinding(enabled);
        cb.iconst_0(); // disabled = false
        cb.labelBinding(stored);
        cb.putstatic(generatedDesc, ASSERTIONS_DISABLED_FIELD, CD_boolean);
    }

    /**
     * Resolves the native symbol via {@link org.elasticsearch.foreign.SymbolResolver} and stores
     * the resulting {@code MethodHandle} in the static field identified by {@code fieldName}.
     * Handles {@code @CaptureErrno}, {@code @Variadic}, and {@code @Critical} options.
     *
     * <p>The generated bytecode is equivalent to:
     * <pre>{@code
     * MemorySegment addr = resolver.resolve(symbolName, LinkerHelper.defaultLookup());
     * <fieldName> = Linker.nativeLinker().downcallHandle(addr, descriptor, options);
     * }</pre>
     */
    private static void emitMhFieldInit(
        CodeBuilder cb,
        ClassDesc generatedDesc,
        MethodModel nm,
        String symbolResolverClassName,
        String fieldName
    ) {
        boolean hasFallbackAdapter = nm.fallbackAdapterClassName() != null;

        // For @Critical methods with a fallback adapter we need to call
        // LinkerAdapter.adaptCritical(lookup, rawHandle, adapterClass, methodName). Stack-prep
        // the leading lookup arg here, then build the raw handle on top.
        if (hasFallbackAdapter) {
            cb.invokestatic(CD_MethodHandles, "lookup", MethodTypeDesc.of(CD_Lookup));
        }

        // Linker.nativeLinker() -> linker
        cb.invokestatic(CD_Linker, "nativeLinker", MTD_nativeLinker, true);

        // resolver.resolve(symbolName, LinkerHelper.defaultLookup()) -> resolvedSymbol
        ClassDesc resolverDesc = ClassDesc.of(symbolResolverClassName);
        cb.new_(resolverDesc);
        cb.dup();
        cb.invokespecial(resolverDesc, "<init>", MethodTypeDesc.of(CD_void));
        cb.ldc(nm.cSymbol());
        cb.invokestatic(CD_LinkerHelper, "defaultLookup", MTD_defaultLookup);
        cb.invokeinterface(ClassDesc.of(RESOLVER_INTERFACE_FQN), "resolve", MTD_resolve);

        // linker.downcallHandle(resolvedSymbol, descriptor, options)
        emitFunctionDescriptor(cb, nm.returnType(), nm.paramTypes());
        emitLinkerOptions(cb, nm);
        cb.invokeinterface(CD_Linker, "downcallHandle", MTD_downcallHandle_byAddress);

        if (hasFallbackAdapter) {
            cb.ldc(ClassDesc.of(nm.fallbackAdapterClassName()));
            cb.ldc(nm.methodName());
            cb.invokestatic(CD_LinkerAdapter, "adaptCritical", MTD_adaptCritical);
        }

        cb.putstatic(generatedDesc, fieldName, CD_MethodHandle);
    }

    private static void emitFunctionDescriptor(CodeBuilder cb, NativeType returnType, List<NativeType> paramTypes) {
        if (returnType == NativeType.VOID) {
            emitParamLayoutArray(cb, paramTypes);
            cb.invokestatic(CD_FunctionDescriptor, "ofVoid", MTD_FunctionDescriptor_ofVoid, true);
        } else {
            emitValueLayout(cb, returnType.layoutType());
            emitParamLayoutArray(cb, paramTypes);
            cb.invokestatic(CD_FunctionDescriptor, "of", MTD_FunctionDescriptor_of, true);
        }
    }

    private static void emitParamLayoutArray(CodeBuilder cb, List<NativeType> paramTypes) {
        cb.loadConstant(paramTypes.size());
        cb.anewarray(CD_MemoryLayout);
        for (int i = 0; i < paramTypes.size(); i++) {
            cb.dup();
            cb.loadConstant(i);
            emitValueLayout(cb, paramTypes.get(i).layoutType());
            cb.aastore();
        }
    }

    /**
     * Emits the {@code Linker.Option[]} array passed to {@code linker.downcallHandle}. For
     * {@code @Critical} the array is built by {@code LinkerAdapter.critical()}. Otherwise the
     * array is assembled inline with a {@code captureCallState("errno")} entry for
     * {@code @CaptureErrno} and/or a {@code firstVariadicArg(N)} entry for {@code @Variadic}
     * (empty when neither applies).
     */
    private static void emitLinkerOptions(CodeBuilder cb, MethodModel nm) {
        if (nm.isCritical()) {
            cb.invokestatic(CD_LinkerAdapter, "critical", MTD_critical);
            return;
        }
        boolean captureErrno = nm.capturesErrno();
        boolean variadic = nm.firstVariadicArg() >= 0;
        int size = (captureErrno ? 1 : 0) + (variadic ? 1 : 0);
        cb.loadConstant(size);
        cb.anewarray(CD_LinkerOption);
        int idx = 0;
        if (captureErrno) {
            // Linker.Option.captureCallState is declared as varargs (String...), so its actual
            // JVM descriptor is ([Ljava/lang/String;)Ljava/lang/foreign/Linker$Option;. Build the
            // one-element array explicitly. Linker.Option is an interface, so invokestatic must
            // mark isInterface=true.
            cb.dup();
            cb.loadConstant(idx++);
            cb.iconst_1();
            cb.anewarray(CD_String);
            cb.dup();
            cb.iconst_0();
            cb.ldc("errno");
            cb.aastore();
            cb.invokestatic(
                CD_LinkerOption,
                "captureCallState",
                MethodTypeDesc.of(CD_LinkerOption, ClassDesc.ofDescriptor("[Ljava/lang/String;")),
                true
            );
            cb.aastore();
        }
        if (variadic) {
            cb.dup();
            cb.loadConstant(idx++);
            cb.loadConstant(nm.firstVariadicArg());
            cb.invokestatic(CD_LinkerOption, "firstVariadicArg", MethodTypeDesc.of(CD_LinkerOption, ClassDesc.ofDescriptor("I")), true);
            cb.aastore();
        }
    }

    // -------------------------------------------------------------------------
    // Method body generation
    // -------------------------------------------------------------------------

    private static void emitNativeFunctionMethod(ClassBuilder cb, ClassDesc generatedDesc, MethodModel nm, String fieldName) {
        int accessFlag = nm.isProtected() ? ClassFile.ACC_PROTECTED : ClassFile.ACC_PUBLIC;
        cb.withMethodBody(nm.methodName(), buildJavaMethodDesc(nm), accessFlag, code -> {
            emitBoundsChecks(code, generatedDesc, nm);
            boolean hasStringParams = nm.paramTypes().contains(NativeType.STRING);
            if (hasStringParams) {
                emitNativeFunctionMethodWithStringParams(code, generatedDesc, nm, fieldName);
            } else {
                code.trying(tryBlock -> {
                    emitInvokeExact(tryBlock, generatedDesc, nm, fieldName);
                    emitTypedReturn(tryBlock, nm.returnType());
                }, catchBuilder -> catchBuilder.catchingAll(catchBlock -> {
                    // throw new AssertionError(t) — stack on entry: [t]
                    catchBlock.new_(CD_AssertionError);
                    catchBlock.dup_x1();
                    catchBlock.swap();
                    catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Object));
                    catchBlock.athrow();
                }));
            }
        });
    }

    /**
     * Emits the fixed {@code Objects.checkFromIndexSize(0L, <shape>, segment.byteSize())} template for
     * every {@code @VectorSegment}/{@code @MatrixSegment}-annotated parameter, in parameter order, at
     * the very top of the method body — before the try block, so a failing check propagates its own
     * {@link IndexOutOfBoundsException} rather than being wrapped in {@link AssertionError}.
     */
    private static void emitBoundsChecks(CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        List<NativeType> paramTypes = nm.paramTypes();
        int[] slots = computeParamSlots(paramTypes);
        for (BoundsCheckModel check : nm.boundsChecks()) {
            switch (check) {
                case BoundsCheckModel.VectorSegmentCheck v -> emitVectorSegmentCheck(cb, generatedDesc, paramTypes, slots, v);
                case BoundsCheckModel.MatrixSegmentCheck m -> emitMatrixSegmentCheck(cb, generatedDesc, paramTypes, slots, m);
            }
        }
    }

    /** Computes the local-variable slot of each parameter (slot 0 is {@code this}). */
    private static int[] computeParamSlots(List<NativeType> paramTypes) {
        int[] slots = new int[paramTypes.size()];
        int slot = 1;
        for (int i = 0; i < paramTypes.size(); i++) {
            slots[i] = slot;
            slot += slotWidth(paramTypes.get(i));
        }
        return slots;
    }

    /**
     * Emits {@code Objects.checkFromIndexSize(0L, ceil(count * elementBits / 8), segment.byteSize())},
     * plus an alignment {@code assert} when {@link BoundsCheckModel.VectorSegmentCheck#aligned()}. The
     * byte size is rounded up so a sub-byte packed vector always has enough whole bytes to hold every
     * element (e.g. {@code 6} 2-bit elements need {@code ceil(12/8) = 2} bytes, not {@code 1}).
     */
    private static void emitVectorSegmentCheck(
        CodeBuilder cb,
        ClassDesc generatedDesc,
        List<NativeType> paramTypes,
        int[] slots,
        BoundsCheckModel.VectorSegmentCheck check
    ) {
        cb.lconst_0();
        emitLongParamLoad(cb, paramTypes.get(check.countParamIndex()), slots[check.countParamIndex()]);
        cb.ldc((long) check.elementBits());
        cb.lmul();
        // ceil(count * elementBits / 8): round the bit count up to whole bytes via (bits + 7) / 8
        cb.ldc(7L);
        cb.ladd();
        cb.ldc(8L);
        cb.ldiv();
        emitCheckFromIndexSize(cb, slots[check.segParamIndex()]);
        if (check.aligned()) {
            emitAlignmentAssert(cb, generatedDesc, slots[check.segParamIndex()], check.elementBits() / 8);
        }
    }

    /**
     * Emits code checking {@code segment.byteSize() < rows * rowBytes}, where
     * {@code rowBytes = ceil(cols * elementBits / 8) + paddingBytes}, plus an alignment {@code assert}
     * when {@link BoundsCheckModel.MatrixSegmentCheck#aligned()}. A 2D segment is treated as {@code rows}
     * independently packed vectors, so each row's byte size is rounded up to whole bytes before the
     * per-row padding is added and the whole thing is multiplied by {@code rows}.
     */
    private static void emitMatrixSegmentCheck(
        CodeBuilder cb,
        ClassDesc generatedDesc,
        List<NativeType> paramTypes,
        int[] slots,
        BoundsCheckModel.MatrixSegmentCheck check
    ) {
        // fromIndex arg for checkFromIndexSize
        cb.lconst_0();

        // ceil(cols * elementBits / 8): round the per-row bit count up to whole bytes via (bits + 7) / 8
        emitLongParamLoad(cb, paramTypes.get(check.colsParamIndex()), slots[check.colsParamIndex()]);
        cb.ldc((long) check.elementBits());
        cb.lmul();
        cb.ldc(7L);
        cb.ladd();
        cb.ldc(8L);
        cb.ldiv();
        if (check.hasPaddingBytes()) {
            emitPaddingBytesRelationalCheck(cb, paramTypes, slots, check);
            emitLongParamLoad(cb, paramTypes.get(check.paddingBytesParamIndex()), slots[check.paddingBytesParamIndex()]);
            cb.ladd();
        }
        emitLongParamLoad(cb, paramTypes.get(check.rowsParamIndex()), slots[check.rowsParamIndex()]);
        cb.lmul();
        emitCheckFromIndexSize(cb, slots[check.segParamIndex()]);
        if (check.aligned()) {
            emitAlignmentAssert(cb, generatedDesc, slots[check.segParamIndex()], check.elementBits() / 8);
        }
    }

    /** Emits {@code if (paddingBytes < 0) throw new IllegalArgumentException(...)}. Stack-neutral. */
    private static void emitPaddingBytesRelationalCheck(
        CodeBuilder cb,
        List<NativeType> paramTypes,
        int[] slots,
        BoundsCheckModel.MatrixSegmentCheck check
    ) {
        var paddingOk = cb.newLabel();
        emitLongParamLoad(cb, paramTypes.get(check.paddingBytesParamIndex()), slots[check.paddingBytesParamIndex()]);
        cb.lconst_0();
        cb.lcmp();
        cb.ifge(paddingOk); // paddingBytes >= 0, ok
        cb.new_(CD_IllegalArgumentException);
        cb.dup();
        cb.ldc("paddingBytes must be >= 0");
        cb.invokespecial(CD_IllegalArgumentException, "<init>", MethodTypeDesc.of(CD_void, CD_String));
        cb.athrow();
        cb.labelBinding(paddingOk);
    }

    /** Stack on entry: {@code [0L, size]}. Pushes {@code segment.byteSize()}, calls the check, discards the result. */
    private static void emitCheckFromIndexSize(CodeBuilder cb, int segSlot) {
        cb.aload(segSlot);
        cb.invokeinterface(CD_MemorySegment, "byteSize", MTD_byteSize);
        cb.invokestatic(CD_Objects, "checkFromIndexSize", MTD_checkFromIndexSize);
        cb.pop2();
    }

    /**
     * Emits {@code assert segment.address() % alignment == 0}, gated by
     * {@link #ASSERTIONS_DISABLED_FIELD} exactly like a source-level {@code assert} statement would
     * compile — skipped entirely under normal execution, zero production cost, and throwing
     * {@link AssertionError} only when assertions are enabled and the segment is misaligned.
     */
    private static void emitAlignmentAssert(CodeBuilder cb, ClassDesc generatedDesc, int segSlot, int alignment) {
        var skip = cb.newLabel();
        cb.getstatic(generatedDesc, ASSERTIONS_DISABLED_FIELD, CD_boolean);
        cb.ifne(skip);
        cb.aload(segSlot);
        cb.invokeinterface(CD_MemorySegment, "address", MethodTypeDesc.of(CD_long));
        cb.ldc((long) alignment);
        cb.lrem();
        cb.lconst_0();
        cb.lcmp();
        cb.ifeq(skip); // remainder == 0 -> aligned, skip the throw
        cb.new_(CD_AssertionError);
        cb.dup();
        cb.ldc("segment not aligned to " + alignment + "-byte boundary");
        cb.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Object));
        cb.athrow();
        cb.labelBinding(skip);
    }

    /** Loads an {@code int}/{@code long} parameter and widens it to {@code long} on the stack. */
    private static void emitLongParamLoad(CodeBuilder cb, NativeType type, int slot) {
        switch (type) {
            case INT -> {
                cb.iload(slot);
                cb.i2l();
            }
            case LONG -> cb.lload(slot);
            case SHORT, BYTE, BOOLEAN, FLOAT, DOUBLE, ADDRESS, STRING, VOID -> throw new AssertionError(
                "shape check operand must be int or long, got: " + type
            );
        }
    }

    /**
     * Generates a method body that marshals {@code String} parameters to native memory before the call.
     * Opens a confined {@code Arena} per call, allocates each {@code String} param via
     * {@code MemorySegmentUtil.allocateString}, and closes the arena in both normal and exception paths.
     *
     * <p>Local variable layout (slots):
     * <ul>
     *   <li>0: {@code this}</li>
     *   <li>1..paramEnd-1: original Java parameters</li>
     *   <li>paramEnd: the {@code Arena}</li>
     *   <li>paramEnd+1..: one {@code MemorySegment} per {@code STRING} parameter, in order</li>
     *   <li>last slot (if non-void return): the return value from invokeExact</li>
     * </ul>
     */
    private static void emitNativeFunctionMethodWithStringParams(
        CodeBuilder code,
        ClassDesc generatedDesc,
        MethodModel nm,
        String fieldName
    ) {
        List<NativeType> paramTypes = nm.paramTypes();
        NativeType returnType = nm.returnType();

        // Compute where params end and arena+marshaled-string locals begin.
        int paramSlotsEnd = 1; // slot 0 = this
        for (NativeType t : paramTypes) {
            paramSlotsEnd += slotWidth(t);
        }
        int arenaSlot = paramSlotsEnd;

        // Count STRING params to know how many marshaled slots we need.
        long stringParamCount = paramTypes.stream().filter(t -> t == NativeType.STRING).count();
        int resultSlot = arenaSlot + 1 + (int) stringParamCount;

        // Arena arena = Arena.ofConfined()
        code.invokestatic(CD_Arena, "ofConfined", MTD_Arena_ofConfined, true);
        code.astore(arenaSlot);

        code.trying(tryBlock -> {
            // Marshal each String param: MemorySegment $sN = MemorySegmentUtil.allocateString(arena, strN)
            int slot = 1;
            int marshaledSlot = arenaSlot + 1;
            for (NativeType paramType : paramTypes) {
                if (paramType == NativeType.STRING) {
                    var notNull = tryBlock.newLabel();
                    var end = tryBlock.newLabel();
                    tryBlock.aload(slot);
                    tryBlock.ifnonnull(notNull);
                    tryBlock.getstatic(CD_MemorySegment, "NULL", CD_MemorySegment);
                    tryBlock.goto_(end);
                    tryBlock.labelBinding(notNull);
                    tryBlock.aload(arenaSlot);
                    tryBlock.aload(slot);
                    tryBlock.invokestatic(CD_MemorySegmentAdapter, "allocateString", MTD_MemorySegmentAdapter_allocateString);
                    tryBlock.labelBinding(end);
                    tryBlock.astore(marshaledSlot);
                    marshaledSlot++;
                }
                slot += slotWidth(paramType);
            }

            // Push method handle, then all params (String params → their marshaled MemorySegment slots)
            tryBlock.getstatic(generatedDesc, fieldName, CD_MethodHandle);
            if (nm.capturesErrno()) {
                tryBlock.getstatic(CD_LinkerHelper, "ERRNO_STATE", CD_MemorySegment);
            }
            slot = 1;
            marshaledSlot = arenaSlot + 1;
            for (NativeType paramType : paramTypes) {
                if (paramType == NativeType.STRING) {
                    tryBlock.aload(marshaledSlot);
                    marshaledSlot++;
                    slot++;
                } else {
                    slot += emitLoadParam(tryBlock, paramType, slot);
                }
            }
            tryBlock.invokevirtual(CD_MethodHandle, "invokeExact", buildInvokeExactDesc(nm));

            // Store return value before closing the arena (avoids having a live value on the
            // stack when we call arena.close()).
            if (returnType != NativeType.VOID) {
                emitStore(tryBlock, returnType, resultSlot);
            }

            tryBlock.aload(arenaSlot);
            tryBlock.invokeinterface(CD_Arena, "close", MTD_Arena_close);

            if (returnType != NativeType.VOID) {
                emitLoad(tryBlock, returnType, resultSlot);
            }
            emitTypedReturn(tryBlock, returnType);
        }, catchBuilder -> catchBuilder.catchingAll(catchBlock -> {
            // Stack on entry: [t]. Close arena, then wrap in AssertionError.
            catchBlock.aload(arenaSlot);
            catchBlock.invokeinterface(CD_Arena, "close", MTD_Arena_close);
            catchBlock.new_(CD_AssertionError);
            catchBlock.dup_x1();
            catchBlock.swap();
            catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Object));
            catchBlock.athrow();
        }));
    }

    /** Stores the top-of-stack value (of the given native type) into a local variable slot. */
    private static void emitStore(CodeBuilder cb, NativeType type, int slot) {
        switch (type) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.istore(slot);
            case LONG -> cb.lstore(slot);
            case FLOAT -> cb.fstore(slot);
            case DOUBLE -> cb.dstore(slot);
            case ADDRESS, STRING -> cb.astore(slot);
            default -> throw new AssertionError("Cannot store type: " + type);
        }
    }

    /** Loads a value of the given native type from a local variable slot onto the stack. */
    private static void emitLoad(CodeBuilder cb, NativeType type, int slot) {
        switch (type) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.iload(slot);
            case LONG -> cb.lload(slot);
            case FLOAT -> cb.fload(slot);
            case DOUBLE -> cb.dload(slot);
            case ADDRESS, STRING -> cb.aload(slot);
            default -> throw new AssertionError("Cannot load type: " + type);
        }
    }

    /**
     * Invokes the native function through its downcall MethodHandle. Prepends {@code ERRNO_STATE}
     * when {@code @CaptureErrno} is present.
     */
    private static void emitInvokeExact(CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm, String fieldName) {
        cb.getstatic(generatedDesc, fieldName, CD_MethodHandle);

        if (nm.capturesErrno()) {
            cb.getstatic(CD_LinkerHelper, "ERRNO_STATE", CD_MemorySegment);
        }

        int slot = 1;
        for (var paramType : nm.paramTypes()) {
            slot += emitLoadParam(cb, paramType, slot);
        }

        cb.invokevirtual(CD_MethodHandle, "invokeExact", buildInvokeExactDesc(nm));
    }

    /**
     * Loads a single parameter onto the stack. Returns the number of local-variable slots consumed
     * (2 for {@code long}/{@code double}, 1 for everything else).
     */
    private static int emitLoadParam(CodeBuilder cb, NativeType paramType, int slot) {
        switch (paramType) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.iload(slot);
            case LONG -> cb.lload(slot);
            case FLOAT -> cb.fload(slot);
            case DOUBLE -> cb.dload(slot);
            case ADDRESS -> cb.aload(slot);
            case ADDRESSABLE -> {
                // Convert Addressable -> long: null becomes 0L, otherwise call segment().address()
                var notNull = cb.newLabel();
                var end = cb.newLabel();
                cb.aload(slot);
                cb.ifnonnull(notNull);
                cb.lconst_0();
                cb.goto_(end);
                cb.labelBinding(notNull);
                cb.aload(slot);
                cb.invokeinterface(CD_Addressable, "segment", MethodTypeDesc.of(CD_MemorySegment));
                cb.invokeinterface(CD_MemorySegment, "address", MethodTypeDesc.of(CD_long));
                cb.labelBinding(end);
            }
            default -> throw new AssertionError("Unhandled param type: " + paramType);
        }
        return slotWidth(paramType);
    }

    private static void emitTypedReturn(CodeBuilder cb, NativeType returnType) {
        switch (returnType) {
            case VOID -> cb.return_();
            case INT, SHORT, BYTE, BOOLEAN -> cb.ireturn();
            case LONG -> cb.lreturn();
            case FLOAT -> cb.freturn();
            case DOUBLE -> cb.dreturn();
            case ADDRESS -> cb.areturn();
            case STRING -> emitStringReturn(cb);
        }
    }

    /**
     * Marshals a {@code MemorySegment} returned by the native call into a Java {@code String},
     * returning {@code null} for a null pointer. Stack on entry: {@code [segment]}.
     */
    private static void emitStringReturn(CodeBuilder cb) {
        var notNull = cb.newLabel();
        cb.dup();
        cb.invokeinterface(CD_MemorySegment, "address", MethodTypeDesc.of(CD_long));
        cb.lconst_0();
        cb.lcmp();
        cb.ifne(notNull);
        // null pointer path: pop segment, return null
        cb.pop();
        cb.aconst_null();
        cb.areturn();
        cb.labelBinding(notNull);
        // Otherwise reinterpret the segment to a known size and read it as a UTF-8 string. We route
        // the read through MemorySegmentAdapter so the mrjar shim picks the right API for the runtime
        // JDK (MemorySegment.getString in JDK 22+, getUtf8String in JDK 21).
        cb.ldc(Long.MAX_VALUE);
        cb.invokeinterface(CD_MemorySegment, "reinterpret", MethodTypeDesc.of(CD_MemorySegment, CD_long));
        cb.ldc(0L);
        cb.invokestatic(CD_MemorySegmentAdapter, "getString", MTD_MemorySegmentAdapter_getString);
        cb.areturn();
    }

    // -------------------------------------------------------------------------
    // @StructFactory method body generation
    // -------------------------------------------------------------------------

    /**
     * Generates the body for a {@code @StructFactory} method. The factory allocates a native
     * struct instance and populates its {@code @ArrayField} pointer + length field from the
     * supplied element array.
     */
    private static void emitStructFactoryMethod(ClassBuilder cb, LibraryModel model, MethodModel nm) {
        // Resolve the target struct and its array field from the model
        StructModel targetStruct = model.structs()
            .stream()
            .filter(s -> s.simpleName().equals(nm.structReturnSimpleName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Cannot find struct model for " + nm.structReturnSimpleName()));
        ArrayFieldModel arrayField = targetStruct.fields().stream().<ArrayFieldModel>mapMulti((f, sink) -> {
            if (f instanceof ArrayFieldModel a) {
                sink.accept(a);
            }
        }).findFirst().orElseThrow(() -> new AssertionError("Struct " + nm.structReturnSimpleName() + " has no @ArrayField"));
        NativeType countType = targetStruct.fields().stream().<ScalarFieldModel>mapMulti((f, sink) -> {
            if (f instanceof ScalarFieldModel s && s.name().equals(arrayField.lengthFieldName())) {
                sink.accept(s);
            }
        }).findFirst().orElseThrow(() -> new AssertionError("Missing length field " + arrayField.lengthFieldName())).type();

        // Class descriptors for the generated struct types
        String prefix = model.packageName().isEmpty() ? model.simpleName() : model.packageName() + "." + model.simpleName();
        ClassDesc structImplDesc = ClassDesc.of(prefix + "$" + nm.structReturnSimpleName() + "$Impl");
        ClassDesc packDesc = ClassDesc.of(prefix + "$" + nm.packedElementSimpleName() + "$Pack");
        ClassDesc elementRecordDesc = ClassDesc.of(prefix + "$" + nm.packedElementSimpleName());
        ClassDesc elementArrayDesc = ClassDesc.ofDescriptor("[L" + elementRecordDesc.descriptorString().substring(1));
        ClassDesc structInterfaceDesc = ClassDesc.of(prefix + "$" + nm.structReturnSimpleName());

        // Method descriptor: (ElementType[]) -> StructInterface
        MethodTypeDesc methodDesc = MethodTypeDesc.of(structInterfaceDesc, elementArrayDesc);

        cb.withMethodBody(nm.methodName(), methodDesc, nm.isProtected() ? ClassFile.ACC_PROTECTED : ClassFile.ACC_PUBLIC, code -> {
            // slot 0 = this, slot 1 = elements (ElementType[])
            // result = new SockFProg$Impl()
            code.new_(structImplDesc);
            code.dup();
            code.invokespecial(structImplDesc, "<init>", MethodTypeDesc.of(CD_void));
            code.astore(2); // slot 2 = result

            // arr = ArenaAdapter.allocate(Arena.ofAuto(), ElemPack.LAYOUT, elements.length)
            // Route through ArenaAdapter: the (MemoryLayout, long) allocate overload is JDK 22+,
            // so the adapter uses allocateArray on JDK 21 and allocate on JDK 22+.
            code.invokestatic(CD_Arena, "ofAuto", MTD_Arena_ofAuto, true);
            code.getstatic(packDesc, "LAYOUT", CD_StructLayout);
            code.aload(1);
            code.arraylength();
            code.invokestatic(CD_ArenaAdapter, "allocate", MTD_ArenaAdapter_allocate);
            code.astore(3); // slot 3 = arr

            // for (int i = 0; i < elements.length; i++) ElemPack.pack(elements[i], arr, LAYOUT.byteSize() * i)
            code.iconst_0();
            code.istore(4); // slot 4 = i

            var loopStart = code.newLabel();
            var loopEnd = code.newLabel();

            code.labelBinding(loopStart);
            code.iload(4);
            code.aload(1);
            code.arraylength();
            code.if_icmpge(loopEnd);

            code.aload(1);
            code.iload(4);
            code.aaload();
            code.aload(3);
            code.getstatic(packDesc, "LAYOUT", CD_StructLayout);
            code.invokeinterface(CD_MemoryLayout, "byteSize", MTD_byteSize);
            code.iload(4);
            code.i2l();
            code.lmul();
            code.invokestatic(packDesc, "pack", MethodTypeDesc.of(CD_void, elementRecordDesc, CD_MemorySegment, CD_long));

            code.iinc(4, 1);
            code.goto_(loopStart);
            code.labelBinding(loopEnd);

            // result.<lengthField>$vh.set(result.segment, (<countType>) elements.length)
            code.getstatic(structImplDesc, arrayField.lengthFieldName() + "$vh", CD_VarHandle);
            code.aload(2);
            code.getfield(structImplDesc, "segment", CD_MemorySegment);
            code.aload(1);
            code.arraylength();
            switch (countType) {
                case SHORT -> code.i2s();
                case INT -> {
                } // arraylength already produces int
                case LONG -> code.i2l();
                case BYTE -> code.i2b();
                default -> throw new AssertionError("Unexpected count type: " + countType);
            }
            ClassDesc countClassDesc = primitiveClassDesc(countType);
            code.invokevirtual(CD_VarHandle, "set", MethodTypeDesc.of(CD_void, CD_MemorySegment, countClassDesc));

            // result.<arrayField>$ptr$vh.set(result.segment, arr)
            code.getstatic(structImplDesc, StructImplWriter.varHandleFieldName(arrayField), CD_VarHandle);
            code.aload(2);
            code.getfield(structImplDesc, "segment", CD_MemorySegment);
            code.aload(3);
            code.invokevirtual(CD_VarHandle, "set", MethodTypeDesc.of(CD_void, CD_MemorySegment, CD_MemorySegment));

            // return result
            code.aload(2);
            code.areturn();
        });
    }

    // -------------------------------------------------------------------------
    // Descriptor helpers
    // -------------------------------------------------------------------------

    /** Builds the Java-facing method descriptor, using Java types for all parameters and the return type. */
    private static MethodTypeDesc buildJavaMethodDesc(MethodModel nm) {
        List<ClassDesc> paramDescs = new ArrayList<>();
        for (var paramType : nm.paramTypes()) {
            paramDescs.add(javaClassDesc(paramType));
        }
        return MethodTypeDesc.of(javaClassDesc(nm.returnType()), paramDescs);
    }

    /**
     * Builds the native-side descriptor for {@code MethodHandle.invokeExact}. When {@code @CaptureErrno}
     * is present, prepends {@code MemorySegment} (for {@code ERRNO_STATE}).
     */
    private static MethodTypeDesc buildInvokeExactDesc(MethodModel nm) {
        List<ClassDesc> paramDescs = new ArrayList<>();
        if (nm.capturesErrno()) {
            paramDescs.add(CD_MemorySegment);
        }
        for (var paramType : nm.paramTypes()) {
            paramDescs.add(nativeClassDesc(paramType));
        }
        return MethodTypeDesc.of(nativeClassDesc(nm.returnType()), paramDescs);
    }

    private static ClassDesc javaClassDesc(NativeType type) {
        return switch (type) {
            case VOID -> CD_void;
            case ADDRESS -> CD_MemorySegment;
            case STRING -> CD_String;
            case ADDRESSABLE -> CD_Addressable;
            default -> primitiveClassDesc(type);
        };
    }

    private static ClassDesc nativeClassDesc(NativeType type) {
        return switch (type) {
            case VOID -> CD_void;
            case ADDRESS, STRING -> CD_MemorySegment;
            case ADDRESSABLE -> CD_long;
            default -> primitiveClassDesc(type);
        };
    }
}
