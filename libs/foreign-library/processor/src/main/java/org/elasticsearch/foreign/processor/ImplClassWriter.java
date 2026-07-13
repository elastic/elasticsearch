/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.ArrayFieldModel;
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
import java.util.List;

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
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_ArenaAdapter_allocate;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_Arena_ofAuto;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_byteSize;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.primitiveClassDesc;
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
    private static final ClassDesc CD_Throwable = ClassDesc.of("java.lang.Throwable");
    private static final ClassDesc CD_Class = ClassDesc.of("java.lang.Class");
    private static final ClassDesc CD_Linker = ClassDesc.of("java.lang.foreign.Linker");
    private static final ClassDesc CD_SymbolLookup = ClassDesc.of("java.lang.foreign.SymbolLookup");
    private static final ClassDesc CD_LinkerHelper = ClassDesc.of("org.elasticsearch.foreign.LinkerHelper");
    private static final ClassDesc CD_LinkerAdapter = ClassDesc.of("org.elasticsearch.foreign.adapter.LinkerAdapter");
    private static final ClassDesc CD_LoaderHelper = ClassDesc.of("org.elasticsearch.foreign.LoaderHelper");

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
        ClassDesc interfaceDesc = ClassDesc.of(model.qualifiedName());
        List<MethodModel> nativeMethods = model.methods();

        // Collect only non-struct-factory methods (methods with @Function)
        List<MethodModel> functionMethods = nativeMethods.stream().filter(m -> m.isStructFactory() == false).toList();

        byte[] classBytes = ClassFile.of().build(generatedDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);
            cb.withInterfaceSymbols(interfaceDesc);

            // MethodHandle fields: one per @Function method
            for (var nm : functionMethods) {
                cb.withField(
                    nm.methodHandleFieldName(),
                    CD_MethodHandle,
                    fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL)
                );
            }

            // <clinit>: load the library and initialize each MethodHandle field
            cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
                if (model.libraryName().isEmpty() == false) {
                    emitLoadLibrary(clinit, model.libraryName());
                }
                for (var nm : functionMethods) {
                    emitMhFieldInit(clinit, generatedDesc, nm, model.symbolResolverClassName());
                }
                clinit.return_();
            });

            // <init>: package-private no-arg constructor
            cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), 0, init -> {
                init.aload(0);
                init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                init.return_();
            });

            // @Function method implementations
            for (var nm : functionMethods) {
                emitNativeFunctionMethod(cb, generatedDesc, nm);
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

    // -------------------------------------------------------------------------
    // <clinit> helpers
    // -------------------------------------------------------------------------

    private static void emitLoadLibrary(CodeBuilder cb, String libName) {
        cb.ldc(libName);
        cb.invokestatic(CD_LoaderHelper, "loadLibrary", MethodTypeDesc.of(CD_void, CD_String));
    }

    /**
     * Resolves the native symbol via {@link org.elasticsearch.foreign.SymbolResolver} and stores
     * the resulting {@code MethodHandle} in the static {@code <name>$mh} field. Handles
     * {@code @CaptureErrno}, {@code @Variadic}, and {@code @Critical} options.
     *
     * <p>The generated bytecode is equivalent to:
     * <pre>{@code
     * MemorySegment addr = resolver.resolve(symbolName, LinkerHelper.defaultLookup());
     * foo$mh = Linker.nativeLinker().downcallHandle(addr, descriptor, options);
     * }</pre>
     */
    private static void emitMhFieldInit(CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm, String symbolResolverClassName) {
        boolean hasFallbackAdapter = nm.fallbackAdapterClassName() != null;

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

        cb.putstatic(generatedDesc, nm.methodHandleFieldName(), CD_MethodHandle);
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

    private static void emitNativeFunctionMethod(ClassBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        cb.withMethodBody(nm.methodName(), buildJavaMethodDesc(nm), ClassFile.ACC_PUBLIC, code -> {
            boolean hasStringParams = nm.paramTypes().contains(NativeType.STRING);
            if (hasStringParams) {
                emitNativeFunctionMethodWithStringParams(code, generatedDesc, nm);
            } else {
                code.trying(tryBlock -> {
                    emitInvokeExact(tryBlock, generatedDesc, nm);
                    emitTypedReturn(tryBlock, nm.returnType());
                }, catchBuilder -> catchBuilder.catchingAll(catchBlock -> {
                    catchBlock.new_(CD_AssertionError);
                    catchBlock.dup_x1();
                    catchBlock.swap();
                    catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Throwable));
                    catchBlock.athrow();
                }));
            }
        });
    }

    private static void emitNativeFunctionMethodWithStringParams(CodeBuilder code, ClassDesc generatedDesc, MethodModel nm) {
        List<NativeType> paramTypes = nm.paramTypes();
        NativeType returnType = nm.returnType();

        int paramSlotsEnd = 1;
        for (NativeType t : paramTypes) {
            paramSlotsEnd += (t == NativeType.LONG || t == NativeType.DOUBLE) ? 2 : 1;
        }
        int arenaSlot = paramSlotsEnd;

        long stringParamCount = paramTypes.stream().filter(t -> t == NativeType.STRING).count();
        int resultSlot = arenaSlot + 1 + (int) stringParamCount;

        code.invokestatic(CD_Arena, "ofConfined", MTD_Arena_ofConfined, true);
        code.astore(arenaSlot);

        code.trying(tryBlock -> {
            int slot = 1;
            int marshaledSlot = arenaSlot + 1;
            for (NativeType paramType : paramTypes) {
                if (paramType == NativeType.STRING) {
                    tryBlock.aload(arenaSlot);
                    tryBlock.aload(slot);
                    tryBlock.invokestatic(CD_MemorySegmentAdapter, "allocateString", MTD_MemorySegmentAdapter_allocateString);
                    tryBlock.astore(marshaledSlot);
                    marshaledSlot++;
                }
                slot += (paramType == NativeType.LONG || paramType == NativeType.DOUBLE) ? 2 : 1;
            }

            tryBlock.getstatic(generatedDesc, nm.methodHandleFieldName(), CD_MethodHandle);
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
            catchBlock.aload(arenaSlot);
            catchBlock.invokeinterface(CD_Arena, "close", MTD_Arena_close);
            catchBlock.new_(CD_AssertionError);
            catchBlock.dup_x1();
            catchBlock.swap();
            catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Throwable));
            catchBlock.athrow();
        }));
    }

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
    private static void emitInvokeExact(CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        cb.getstatic(generatedDesc, nm.methodHandleFieldName(), CD_MethodHandle);

        if (nm.capturesErrno()) {
            cb.getstatic(CD_LinkerHelper, "ERRNO_STATE", CD_MemorySegment);
        }

        int slot = 1;
        for (var paramType : nm.paramTypes()) {
            slot += emitLoadParam(cb, paramType, slot);
        }

        cb.invokevirtual(CD_MethodHandle, "invokeExact", buildInvokeExactDesc(nm));
    }

    private static int emitLoadParam(CodeBuilder cb, NativeType paramType, int slot) {
        switch (paramType) {
            case INT, SHORT, BYTE, BOOLEAN -> {
                cb.iload(slot);
                return 1;
            }
            case LONG -> {
                cb.lload(slot);
                return 2;
            }
            case FLOAT -> {
                cb.fload(slot);
                return 1;
            }
            case DOUBLE -> {
                cb.dload(slot);
                return 2;
            }
            case ADDRESS -> {
                cb.aload(slot);
                return 1;
            }
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
                return 1;
            }
            default -> throw new AssertionError("Unhandled param type: " + paramType);
        }
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

    private static void emitStringReturn(CodeBuilder cb) {
        var notNull = cb.newLabel();
        cb.dup();
        cb.invokeinterface(CD_MemorySegment, "address", MethodTypeDesc.of(CD_long));
        cb.lconst_0();
        cb.lcmp();
        cb.ifne(notNull);
        cb.pop();
        cb.aconst_null();
        cb.areturn();
        cb.labelBinding(notNull);
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

        cb.withMethodBody(nm.methodName(), methodDesc, ClassFile.ACC_PUBLIC, code -> {
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
