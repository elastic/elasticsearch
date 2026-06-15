/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.FieldModel;
import org.elasticsearch.foreign.processor.model.LibraryModel;
import org.elasticsearch.foreign.processor.model.MethodModel;
import org.elasticsearch.foreign.processor.model.NativeType;
import org.elasticsearch.foreign.processor.model.StructModel;

import java.lang.classfile.ClassFile;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.Filer;
import javax.lang.model.element.TypeElement;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Arena;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayoutArray;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegment;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Object;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_String;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_StructLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.fieldClassDesc;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.primitiveClassDesc;

/**
 * Generates {@code <InterfaceName>$Impl} class files for {@code @LibrarySpecification}-annotated interfaces.
 *
 * <p>Each generated class:
 * <ul>
 *   <li>is package-private {@code final} with a package-private no-arg constructor (only {@code $Provider} in the same package can instantiate it)</li>
 *   <li>implements the annotated interface</li>
 *   <li>has one {@code private static final MethodHandle} field per {@code @Function} or {@code @FunctionPointer} method</li>
 *   <li>initializes those fields in {@code <clinit>}</li>
 *   <li>implements each interface method by calling {@code MethodHandle.invokeExact}</li>
 * </ul>
 */
class ImplClassWriter {

    private static final ClassDesc CD_MethodHandle = ClassDesc.of("java.lang.invoke.MethodHandle");
    private static final ClassDesc CD_FunctionDescriptor = ClassDesc.of("java.lang.foreign.FunctionDescriptor");
    private static final ClassDesc CD_LinkerOption = ClassDesc.of("java.lang.foreign.Linker$Option");
    private static final ClassDesc CD_LinkerOptionArray = ClassDesc.ofDescriptor("[Ljava/lang/foreign/Linker$Option;");
    private static final ClassDesc CD_AssertionError = ClassDesc.of("java.lang.AssertionError");
    private static final ClassDesc CD_Throwable = ClassDesc.of("java.lang.Throwable");
    private static final ClassDesc CD_LinkerHelper = ClassDesc.of("org.elasticsearch.foreign.LinkerHelper");
    private static final ClassDesc CD_LinkerHelperUtil = ClassDesc.of("org.elasticsearch.foreign.LinkerHelperUtil");
    private static final ClassDesc CD_NativeSymbolResolver = ClassDesc.of("org.elasticsearch.foreign.SymbolResolver");
    private static final ClassDesc CD_LoaderHelper = ClassDesc.of("org.elasticsearch.foreign.LoaderHelper");
    private static final ClassDesc CD_SymbolLookup = ClassDesc.of("java.lang.foreign.SymbolLookup");
    private static final ClassDesc CD_MemoryLayoutPathElementArray = ClassDesc.ofDescriptor(
        "[Ljava/lang/foreign/MemoryLayout$PathElement;"
    );
    private static final ClassDesc CD_int = ClassDesc.ofDescriptor("I");
    private static final ClassDesc CD_Class = ClassDesc.of("java.lang.Class");
    private static final ClassDesc CD_MethodHandles = ClassDesc.of("java.lang.invoke.MethodHandles");
    private static final ClassDesc CD_MethodHandlesLookup = ClassDesc.of("java.lang.invoke.MethodHandles$Lookup");
    private final Filer filer;
    private final int classFileVersion;

    ImplClassWriter(Filer filer, int classFileVersion) {
        this.filer = filer;
        this.classFileVersion = classFileVersion;
    }

    /**
     * Generates and writes the {@code $Impl} class for the given library model.
     */
    void generate(LibraryModel model, TypeElement sourceElement) throws Exception {
        String generatedName = model.packageName().isEmpty()
            ? model.simpleName() + "$Impl"
            : model.packageName() + "." + model.simpleName() + "$Impl";
        ClassDesc generatedDesc = ClassDesc.of(generatedName);
        ClassDesc interfaceDesc = ClassDesc.of(model.qualifiedName());

        List<MethodModel.FunctionMethod> nativeMethods = new ArrayList<>();
        List<MethodModel.FunctionPointerMethod> functionPointerMethods = new ArrayList<>();
        for (MethodModel m : model.methods()) {
            if (m instanceof MethodModel.FunctionMethod nm) {
                nativeMethods.add(nm);
            } else if (m instanceof MethodModel.FunctionPointerMethod fp) {
                functionPointerMethods.add(fp);
            }
        }

        boolean hasCaptureErrno = nativeMethods.stream().anyMatch(MethodModel.FunctionMethod::capturesErrno)
            || functionPointerMethods.stream().anyMatch(MethodModel.FunctionPointerMethod::capturesErrno);

        byte[] classBytes = ClassFile.of().build(generatedDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);
            cb.withInterfaceSymbols(interfaceDesc);

            // errnoState field (only if any method uses @CaptureErrno)
            if (hasCaptureErrno) {
                cb.withField("errnoState", CD_MemorySegment, fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL));
            }

            // MethodHandle fields: one per @Function or @FunctionPointer method
            for (var nm : nativeMethods) {
                cb.withField(
                    nm.methodName() + "$mh",
                    CD_MethodHandle,
                    fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL)
                );
            }
            for (var fp : functionPointerMethods) {
                cb.withField(
                    fp.methodName() + "$mh",
                    CD_MethodHandle,
                    fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL)
                );
            }

            // <clinit>: initialize all static fields
            cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
                if (model.libraryName().isEmpty() == false) {
                    emitLoadLibrary(clinit, model.libraryName());
                }
                if (hasCaptureErrno) {
                    emitErrnoStateInit(clinit, generatedDesc);
                }
                for (var nm : nativeMethods) {
                    emitMhFieldInit(clinit, generatedDesc, nm);
                }
                for (var fp : functionPointerMethods) {
                    emitMhFieldInit(clinit, generatedDesc, fp);
                }
                clinit.return_();
            });

            // <init>: package-private no-arg constructor (only $Provider in the same package calls this)
            cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), 0, init -> {
                init.aload(0);
                init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                init.return_();
            });

            // @Function method implementations
            for (var nm : nativeMethods) {
                emitNativeFunctionMethod(cb, generatedDesc, nm);
            }

            // @FunctionPointer method implementations
            for (var fp : functionPointerMethods) {
                emitFunctionPointerMethod(cb, generatedDesc, model, fp);
            }

            // @StructFactory method implementations
            for (MethodModel m : model.methods()) {
                if (m instanceof MethodModel.StructFactoryMethod sf) {
                    emitStructFactoryMethod(cb, generatedDesc, model, sf);
                } else if (m instanceof MethodModel.ArrayFactoryMethod af) {
                    emitArrayFactoryMethod(cb, generatedDesc, model, af);
                }
            }
        });

        try (var os = filer.createClassFile(generatedName, sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    // -------------------------------------------------------------------------
    // <clinit> helpers
    // -------------------------------------------------------------------------

    private static void emitLoadLibrary(java.lang.classfile.CodeBuilder cb, String libName) {
        cb.ldc(libName);
        cb.invokestatic(CD_LoaderHelper, "loadLibrary", MethodTypeDesc.of(CD_void, CD_String));
    }

    /**
     * Allocates the errno capture buffer shared by all {@code @CaptureErrno} methods on this class.
     * Using one segment per class avoids per-call allocation.
     */
    private static void emitErrnoStateInit(java.lang.classfile.CodeBuilder cb, ClassDesc generatedDesc) {
        // errnoState = Arena.ofAuto().allocate(Linker.Option.captureStateLayout())
        // Arena and Linker.Option are interfaces, so isInterface=true
        cb.invokestatic(CD_Arena, "ofAuto", MethodTypeDesc.of(CD_Arena), true);
        cb.invokestatic(CD_LinkerOption, "captureStateLayout", MethodTypeDesc.of(CD_StructLayout), true);
        cb.invokeinterface(CD_Arena, "allocate", MethodTypeDesc.of(CD_MemorySegment, CD_MemoryLayout));
        cb.putstatic(generatedDesc, "errnoState", CD_MemorySegment);
    }

    /**
     * Resolves the native symbol and links it to a MethodHandle stored in {@code <name>$mh}.
     * This handle is what the generated method implementation calls at runtime.
     */
    private static void emitMhFieldInit(java.lang.classfile.CodeBuilder cb, ClassDesc generatedDesc, MethodModel.NativeCallable nm) {
        String fieldName = nm.methodName() + "$mh";

        if (nm.symbolResolverClassName() != null) {
            // Instantiate resolver, call resolve(baseName, defaultLookup()), then downcallHandle
            ClassDesc resolverDesc = ClassDesc.of(nm.symbolResolverClassName());
            cb.new_(resolverDesc);
            cb.dup();
            cb.invokespecial(resolverDesc, "<init>", MethodTypeDesc.of(CD_void));
            cb.ldc(nm.cSymbol());
            cb.invokestatic(CD_LinkerHelper, "defaultLookup", MethodTypeDesc.of(CD_SymbolLookup));
            cb.invokeinterface(CD_NativeSymbolResolver, "resolve", MethodTypeDesc.of(CD_String, CD_String, CD_SymbolLookup));
        } else {
            cb.ldc(nm.cSymbol());
        }

        emitFunctionDescriptor(cb, nm.returnType(), nativeParamTypes(nm));
        emitLinkerOptions(cb, nm);
        emitDowncallHandleCall(cb, nm);
        cb.putstatic(generatedDesc, fieldName, CD_MethodHandle);
    }

    private static void emitFunctionDescriptor(
        java.lang.classfile.CodeBuilder cb,
        MethodModel.ReturnType returnType,
        List<NativeType> paramTypes
    ) {
        int paramCount = paramTypes.size();

        if (returnType.type() == NativeType.VOID) {
            cb.loadConstant(paramCount);
            cb.anewarray(CD_MemoryLayout);
            for (int i = 0; i < paramCount; i++) {
                cb.dup();
                cb.loadConstant(i);
                emitValueLayout(cb, paramTypes.get(i));
                cb.aastore();
            }
            // FunctionDescriptor is an interface; isInterface=true
            cb.invokestatic(CD_FunctionDescriptor, "ofVoid", MethodTypeDesc.of(CD_FunctionDescriptor, CD_MemoryLayoutArray), true);
        } else {
            emitValueLayout(cb, nativeLayoutType(returnType.type()));
            cb.loadConstant(paramCount);
            cb.anewarray(CD_MemoryLayout);
            for (int i = 0; i < paramCount; i++) {
                cb.dup();
                cb.loadConstant(i);
                emitValueLayout(cb, paramTypes.get(i));
                cb.aastore();
            }
            // FunctionDescriptor is an interface; isInterface=true
            cb.invokestatic(
                CD_FunctionDescriptor,
                "of",
                MethodTypeDesc.of(CD_FunctionDescriptor, CD_MemoryLayout, CD_MemoryLayoutArray),
                true
            );
        }
    }

    /**
     * Builds the linker option array for the downcall. Errno capture is handled via a separate
     * downcall handle variant, not as a linker option.
     */
    private static void emitLinkerOptions(java.lang.classfile.CodeBuilder cb, MethodModel.NativeCallable nm) {
        if (nm.isCritical()) {
            cb.invokestatic(CD_LinkerHelperUtil, "critical", MethodTypeDesc.of(CD_LinkerOptionArray));
        } else {
            cb.iconst_0();
            cb.anewarray(CD_LinkerOption);
        }
    }

    private static void emitDowncallHandleCall(java.lang.classfile.CodeBuilder cb, MethodModel.NativeCallable nm) {
        MethodTypeDesc downcallDesc = MethodTypeDesc.of(CD_MethodHandle, CD_String, CD_FunctionDescriptor, CD_LinkerOptionArray);
        if (nm.capturesErrno()) {
            cb.invokestatic(CD_LinkerHelper, "downcallHandleWithErrno", downcallDesc);
        } else {
            cb.invokestatic(CD_LinkerHelper, "downcallHandle", downcallDesc);
        }
    }

    // -------------------------------------------------------------------------
    // Method body generation
    // -------------------------------------------------------------------------

    private static void emitNativeFunctionMethod(
        java.lang.classfile.ClassBuilder cb,
        ClassDesc generatedDesc,
        MethodModel.FunctionMethod nm
    ) {
        MethodTypeDesc javaDesc = buildJavaMethodDesc(nm);
        cb.withMethodBody(nm.methodName(), javaDesc, ClassFile.ACC_PUBLIC, code -> {
            int nextLocalSlot = computeLocalBaseSlot(nm);
            boolean needsCallArena = nm.paramTypes().stream().anyMatch(p -> p.type() == NativeType.STRING);
            int callArenaSlot = needsCallArena ? nextLocalSlot++ : -1;

            if (needsCallArena) {
                // Arena callArena = Arena.ofConfined(); — Arena is an interface, isInterface=true
                code.invokestatic(CD_Arena, "ofConfined", MethodTypeDesc.of(CD_Arena), true);
                code.astore(nextLocalSlot++);
            }

            code.trying(tryBlock -> {
                emitInvokeExact(tryBlock, generatedDesc, nm, callArenaSlot);
                if (needsCallArena) {
                    // close arena on normal path before return
                    tryBlock.aload(callArenaSlot);
                    tryBlock.invokeinterface(CD_Arena, "close", MethodTypeDesc.of(CD_void));
                }
                emitTypedReturn(tryBlock, nm.returnType());
            }, catchBuilder -> catchBuilder.catchingAll(catchBlock -> {
                if (needsCallArena) {
                    // store exception, close arena, rethrow
                    int exSlot = callArenaSlot + 1;
                    catchBlock.astore(exSlot);
                    catchBlock.aload(callArenaSlot);
                    catchBlock.invokeinterface(CD_Arena, "close", MethodTypeDesc.of(CD_void));
                    catchBlock.aload(exSlot);
                }
                // throw new AssertionError(t)
                // Stack: [t]
                catchBlock.new_(CD_AssertionError);
                catchBlock.dup_x1();
                catchBlock.swap();
                catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Throwable));
                catchBlock.athrow();
            }));
        });
    }

    /**
     * Invokes the native function through its downcall MethodHandle, marshaling each parameter to its
     * FFM-compatible type and prepending the errno capture buffer when the method uses {@code @CaptureErrno}.
     */
    private static void emitInvokeExact(
        java.lang.classfile.CodeBuilder cb,
        ClassDesc generatedDesc,
        MethodModel.NativeCallable nm,
        int callArenaSlot
    ) {
        // Load the MethodHandle
        cb.getstatic(generatedDesc, nm.methodName() + "$mh", CD_MethodHandle);

        // Prepend errnoState for @CaptureErrno
        if (nm.capturesErrno()) {
            cb.getstatic(generatedDesc, "errnoState", CD_MemorySegment);
        }

        // Load each parameter
        int slot = 1; // slot 0 = this
        for (var param : nm.paramTypes()) {
            slot += emitLoadParam(cb, param, slot, callArenaSlot);
        }

        // Call invokeExact with the exact native descriptor
        MethodTypeDesc invokeExactDesc = buildInvokeExactDesc(nm);
        cb.invokevirtual(CD_MethodHandle, "invokeExact", invokeExactDesc);
    }

    /**
     * Marshals a single parameter to its FFM-compatible form for the native call.
     * Returns the number of local variable slots consumed (2 for {@code long}/{@code double}, 1 for others).
     */
    private static int emitLoadParam(java.lang.classfile.CodeBuilder cb, MethodModel.ParamInfo param, int slot, int callArenaSlot) {
        switch (param.type()) {
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
            case STRING -> {
                if (param.isUtf16()) {
                    // UTF-16LE encoding: callArena.allocateFrom(StandardCharsets.UTF_16LE, param)
                    cb.aload(callArenaSlot);
                    cb.getstatic(ClassDesc.of("java.nio.charset.StandardCharsets"), "UTF_16LE", ClassDesc.of("java.nio.charset.Charset"));
                    cb.aload(slot);
                    cb.invokeinterface(
                        CD_Arena,
                        "allocateFrom",
                        MethodTypeDesc.ofDescriptor("(Ljava/nio/charset/Charset;Ljava/lang/CharSequence;)Ljava/lang/foreign/MemorySegment;")
                    );
                } else {
                    // UTF-8: callArena.allocateFrom(param)
                    cb.aload(callArenaSlot);
                    cb.aload(slot);
                    cb.invokeinterface(CD_Arena, "allocateFrom", MethodTypeDesc.of(CD_MemorySegment, CD_String));
                }
                return 1;
            }
            case ARENA -> {
                // Arena is not a native argument — skip it (do not push to stack)
                return 1;
            }
            default -> throw new AssertionError("Unhandled param type: " + param.type());
        }
    }

    private static void emitTypedReturn(java.lang.classfile.CodeBuilder cb, MethodModel.ReturnType returnType) {
        if (returnType.type() == NativeType.VOID) {
            cb.return_();
            return;
        }
        switch (returnType.type()) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.ireturn();
            case LONG -> cb.lreturn();
            case FLOAT -> cb.freturn();
            case DOUBLE -> cb.dreturn();
            case ADDRESS -> cb.areturn();
            case STRING -> {
                // invokeExact returned a MemorySegment. If its address is 0 (null pointer), return null.
                // Otherwise call .reinterpret(Long.MAX_VALUE).getString(0).
                // Stack: [segment]
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
                cb.ldc(Long.MAX_VALUE);
                cb.invokeinterface(CD_MemorySegment, "reinterpret", MethodTypeDesc.of(CD_MemorySegment, CD_long));
                cb.ldc(0L);
                if (returnType.isUtf16()) {
                    cb.getstatic(ClassDesc.of("java.nio.charset.StandardCharsets"), "UTF_16LE", ClassDesc.of("java.nio.charset.Charset"));
                    cb.invokeinterface(
                        CD_MemorySegment,
                        "getString",
                        MethodTypeDesc.ofDescriptor("(JLjava/nio/charset/Charset;)Ljava/lang/String;")
                    );
                } else {
                    cb.invokeinterface(CD_MemorySegment, "getString", MethodTypeDesc.of(CD_String, CD_long));
                }
                cb.areturn();
            }
            case ARENA -> throw new AssertionError("ARENA cannot be a return type");
            case VOID -> throw new AssertionError("void handled above");
        }
    }

    // -------------------------------------------------------------------------
    // Descriptor helpers
    // -------------------------------------------------------------------------

    /** Builds the Java-facing method descriptor, using Java types for all parameters and the return type. */
    private static MethodTypeDesc buildJavaMethodDesc(MethodModel.NativeCallable nm) {
        ClassDesc returnDesc = javaClassDesc(nm.returnType());
        List<ClassDesc> paramDescs = new ArrayList<>();
        for (var p : nm.paramTypes()) {
            paramDescs.add(javaParamClassDesc(p));
        }
        return MethodTypeDesc.of(returnDesc, paramDescs);
    }

    /**
     * Builds the native-side descriptor used when calling through the downcall MethodHandle. This differs
     * from the Java-facing descriptor in three ways: Arena params are excluded (they are Java-only lifetime
     * management and have no native counterpart); String params/return become MemorySegment (marshaling
     * happens before/after the call); and a leading MemorySegment is prepended for {@code @CaptureErrno}.
     */
    private static MethodTypeDesc buildInvokeExactDesc(MethodModel.NativeCallable nm) {
        List<ClassDesc> paramDescs = new ArrayList<>();
        if (nm.capturesErrno()) {
            paramDescs.add(CD_MemorySegment);
        }
        for (var p : nm.paramTypes()) {
            if (p.type() == NativeType.ARENA) {
                continue;
            }
            paramDescs.add(nativeClassDesc(p.type()));
        }
        ClassDesc returnDesc;
        if (nm.returnType().type() == NativeType.VOID) {
            returnDesc = CD_void;
        } else if (nm.returnType().type() == NativeType.STRING) {
            returnDesc = CD_MemorySegment;
        } else {
            returnDesc = nativeClassDesc(nm.returnType().type());
        }
        return MethodTypeDesc.of(returnDesc, paramDescs);
    }

    private static ClassDesc javaClassDesc(MethodModel.ReturnType returnType) {
        if (returnType.type() == NativeType.VOID) {
            return CD_void;
        }
        return switch (returnType.type()) {
            case ADDRESS -> CD_MemorySegment;
            case STRING -> CD_String;
            case ARENA -> throw new AssertionError("ARENA cannot be a return type");
            case VOID -> throw new AssertionError("void handled above");
            default -> primitiveClassDesc(returnType.type());
        };
    }

    private static ClassDesc javaParamClassDesc(MethodModel.ParamInfo param) {
        return switch (param.type()) {
            case ADDRESS -> CD_MemorySegment;
            case STRING -> CD_String;
            case ARENA -> CD_Arena;
            case VOID -> throw new AssertionError("void cannot be a parameter type");
            default -> primitiveClassDesc(param.type());
        };
    }

    private static ClassDesc nativeClassDesc(NativeType type) {
        return switch (type) {
            case ADDRESS, STRING -> CD_MemorySegment;
            case ARENA -> throw new AssertionError("ARENA is not a native type");
            case VOID -> throw new AssertionError("void is not a native type");
            default -> primitiveClassDesc(type);
        };
    }

    /** STRING and ADDRESS both occupy an address-sized slot in the native layout. */
    private static NativeType nativeLayoutType(NativeType type) {
        return switch (type) {
            case STRING -> NativeType.ADDRESS;
            case VOID -> throw new AssertionError("void has no native layout");
            default -> type;
        };
    }

    /**
     * Returns the parameter types to include in the native FunctionDescriptor, excluding Arena
     * (a Java-only lifetime management type with no native counterpart). String parameters are
     * preserved as STRING rather than collapsed to ADDRESS so callers retain type information.
     */
    private static List<NativeType> nativeParamTypes(MethodModel.NativeCallable nm) {
        List<NativeType> result = new ArrayList<>();
        for (var param : nm.paramTypes()) {
            if (param.type() != NativeType.ARENA) {
                result.add(param.type());
            }
        }
        return result;
    }

    /**
     * Returns the first local variable slot available after {@code this} and all method parameters.
     * {@code long} and {@code double} parameters each occupy two slots.
     */
    private static int computeLocalBaseSlot(MethodModel.NativeCallable nm) {
        int slot = 1;
        for (var p : nm.paramTypes()) {
            slot += (p.type() == NativeType.LONG || p.type() == NativeType.DOUBLE) ? 2 : 1;
        }
        return slot;
    }

    // -------------------------------------------------------------------------
    // @StructFactory method generation
    // -------------------------------------------------------------------------

    /**
     * Emits a struct factory method for a static or dynamic layout struct:
     * <pre>
     *   // static (no params):
     *   public RLimit newRLimit() { return new RLimit$Impl(); }
     *
     *   // dynamic (sizeof + offsets in declaration order):
     *   public Stat64 newStat64(long sizeof, long st_size$offset, ...) {
     *       return new Stat64$Impl(sizeof, st_size$offset, ...);
     *   }
     * </pre>
     */
    private static void emitStructFactoryMethod(
        java.lang.classfile.ClassBuilder cb,
        ClassDesc generatedDesc,
        LibraryModel libraryModel,
        MethodModel.StructFactoryMethod sf
    ) {
        String outerPrefix = libraryModel.packageName().isEmpty()
            ? libraryModel.simpleName()
            : libraryModel.packageName() + "." + libraryModel.simpleName();

        // The interface type returned: outer.Outer$StructName
        ClassDesc returnInterfaceDesc = ClassDesc.of(outerPrefix + "$" + sf.structTypeName());
        // The impl class: outer.Outer$StructName$Impl
        ClassDesc implDesc = ClassDesc.of(outerPrefix + "$" + sf.structTypeName() + "$Impl");

        if (!sf.isDynamic()) {
            // Static: no params, just new <Name>$Impl()
            MethodTypeDesc methodDesc = MethodTypeDesc.of(returnInterfaceDesc);
            cb.withMethodBody(sf.methodName(), methodDesc, ClassFile.ACC_PUBLIC, code -> {
                code.new_(implDesc);
                code.dup();
                code.invokespecial(implDesc, "<init>", MethodTypeDesc.of(CD_void));
                code.areturn();
            });
        } else {
            // Dynamic: (long sizeof, long off1, long off2, ...)
            List<ClassDesc> paramDescs = new ArrayList<>();
            paramDescs.add(CD_long); // sizeof
            for (int i = 0; i < sf.fieldNames().size(); i++) {
                paramDescs.add(CD_long);
            }
            MethodTypeDesc methodDesc = MethodTypeDesc.of(returnInterfaceDesc, paramDescs);

            // The impl constructor has the same signature: (long sizeof, long off1, ...)
            List<ClassDesc> ctorParams = new ArrayList<>(paramDescs);
            MethodTypeDesc ctorDesc = MethodTypeDesc.of(CD_void, ctorParams);

            cb.withMethodBody(sf.methodName(), methodDesc, ClassFile.ACC_PUBLIC, code -> {
                code.new_(implDesc);
                code.dup();
                // Load sizeof (slot 1, long = 2 slots) then each offset
                int slot = 1;
                for (int i = 0; i <= sf.fieldNames().size(); i++) {
                    code.lload(slot);
                    slot += 2;
                }
                code.invokespecial(implDesc, "<init>", ctorDesc);
                code.areturn();
            });
        }
    }

    /**
     * Emits an array factory method:
     * <pre>
     *   public SockFProg newSockFProg(SockFilter[] filters) {
     *       var result = new SockFProg$Impl();
     *       result.len((short) filters.length);
     *       var arr = Arena.ofAuto().allocate(SockFilter$Impl.LAYOUT, filters.length);
     *       for (int i = 0; i &lt; filters.length; i++) {
     *           MemorySegment.copy(((SockFilter$Impl) filters[i]).segment, 0,
     *               arr.asSlice(SockFilter$Impl.LAYOUT.byteSize() * i,
     *                           SockFilter$Impl.LAYOUT.byteSize()), 0,
     *               SockFilter$Impl.LAYOUT.byteSize());
     *       }
     *       result.filter(arr);
     *       return result;
     *   }
     * </pre>
     *
     * <p>This method identifies which field is the @ArrayOf field (ADDRESS type) and which is
     * the @ArrayLength field. It sets the length field first, then copies elements, then sets
     * the array pointer.
     */
    private static void emitArrayFactoryMethod(
        java.lang.classfile.ClassBuilder cb,
        ClassDesc generatedDesc,
        LibraryModel libraryModel,
        MethodModel.ArrayFactoryMethod af
    ) {
        String outerPrefix = libraryModel.packageName().isEmpty()
            ? libraryModel.simpleName()
            : libraryModel.packageName() + "." + libraryModel.simpleName();

        // Interface types
        ClassDesc returnInterfaceDesc = ClassDesc.of(outerPrefix + "$" + af.structTypeName());
        ClassDesc resultImplDesc = ClassDesc.of(outerPrefix + "$" + af.structTypeName() + "$Impl");
        ClassDesc elementInterfaceDesc = ClassDesc.of(outerPrefix + "$" + af.elementTypeName());
        ClassDesc elementImplDesc = ClassDesc.of(outerPrefix + "$" + af.elementTypeName() + "$Impl");
        ClassDesc elementArrayDesc = ClassDesc.ofDescriptor("[" + elementInterfaceDesc.descriptorString());

        // method signature: (ElementType[]) -> StructType
        MethodTypeDesc methodDesc = MethodTypeDesc.of(returnInterfaceDesc, elementArrayDesc);

        cb.withMethodBody(af.methodName(), methodDesc, ClassFile.ACC_PUBLIC, code -> {
            // Slot map: 0=this, 1=filters array, 2=result, 3=arr (MemorySegment), 4=loop index i
            int filtersSlot = 1;
            int resultSlot = 2;
            int arrSlot = 3;
            int iSlot = 4;

            // var result = new <Struct>$Impl()
            code.new_(resultImplDesc);
            code.dup();
            code.invokespecial(resultImplDesc, "<init>", MethodTypeDesc.of(CD_void));
            code.astore(resultSlot);

            // Find struct model for the return type to know field names/types
            // We need to find the @ArrayLength field name and the @ArrayOf field name from the struct model.
            // We look these up from libraryModel.structs().
            StructModel structModel = libraryModel.structs()
                .stream()
                .filter(s -> s.simpleName().equals(af.structTypeName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No struct model for " + af.structTypeName()));

            String arrayLengthFieldName = null;
            String arrayOfFieldName = null;
            NativeType arrayLengthTypeKind = null;
            for (FieldModel field : structModel.fields()) {
                if (field.arrayLengthFor() != null) {
                    arrayLengthFieldName = field.name();
                    arrayLengthTypeKind = field.nativeType();
                }
                if (field.arrayOf() != null) {
                    arrayOfFieldName = field.name();
                }
            }

            if (arrayLengthFieldName == null || arrayLengthTypeKind == null) {
                throw new AssertionError("ArrayFactoryMethod for '" + af.structTypeName() + "' has no @ArrayLength field");
            }
            if (arrayOfFieldName == null) {
                throw new AssertionError("ArrayFactoryMethod for '" + af.structTypeName() + "' has no @ArrayOf field");
            }
            final String finalArrayLengthFieldName = arrayLengthFieldName;
            final String finalArrayOfFieldName = arrayOfFieldName;
            final NativeType finalArrayLengthTypeKind = arrayLengthTypeKind;

            // result.<arrayLength>((<type>) filters.length)
            code.aload(resultSlot);
            code.aload(filtersSlot);
            code.arraylength();
            ClassDesc lenParamDesc = fieldClassDesc(finalArrayLengthTypeKind);
            if (finalArrayLengthTypeKind == NativeType.SHORT) {
                code.i2s();
            } else if (finalArrayLengthTypeKind == NativeType.BYTE) {
                code.i2b();
            }
            code.invokeinterface(returnInterfaceDesc, finalArrayLengthFieldName, MethodTypeDesc.of(CD_void, lenParamDesc));

            // var arr = Arena.ofAuto().allocate(<Element>$Impl.LAYOUT, filters.length)
            code.invokestatic(CD_Arena, "ofAuto", MethodTypeDesc.of(CD_Arena), true);
            code.getstatic(elementImplDesc, "LAYOUT", CD_StructLayout);
            code.aload(filtersSlot);
            code.arraylength();
            code.i2l();
            code.invokeinterface(CD_Arena, "allocate", MethodTypeDesc.of(CD_MemorySegment, CD_MemoryLayout, CD_long));
            code.astore(arrSlot);

            // for (int i = 0; i < filters.length; i++) { ... copy element ... }
            code.iconst_0();
            code.istore(iSlot);

            emitArrayCopyLoop(code, filtersSlot, arrSlot, iSlot, elementImplDesc);

            // result.<arrayOf>(arr)
            code.aload(resultSlot);
            code.aload(arrSlot);
            code.invokeinterface(returnInterfaceDesc, finalArrayOfFieldName, MethodTypeDesc.of(CD_void, CD_MemorySegment));

            code.aload(resultSlot);
            code.areturn();
        });
    }

    /**
     * Emits the inner for-loop body for array copy:
     * <pre>
     *   for (int i = 0; i &lt; filters.length; i++) {
     *       MemorySegment.copy(
     *           ((Element$Impl) filters[i]).segment, 0L,
     *           arr.asSlice(Element$Impl.LAYOUT.byteSize() * i,
     *                       Element$Impl.LAYOUT.byteSize()),
     *           0L,
     *           Element$Impl.LAYOUT.byteSize());
     *   }
     * </pre>
     */
    private static void emitArrayCopyLoop(
        java.lang.classfile.CodeBuilder code,
        int filtersSlot,
        int arrSlot,
        int iSlot,
        ClassDesc elementImplDesc
    ) {
        java.lang.classfile.Label loopStart = code.newLabel();
        java.lang.classfile.Label loopEnd = code.newLabel();

        // loop condition: i < filters.length
        code.labelBinding(loopStart);
        code.iload(iSlot);
        code.aload(filtersSlot);
        code.arraylength();
        code.if_icmpge(loopEnd);

        // src = ((Element$Impl) filters[i]).segment
        code.aload(filtersSlot);
        code.iload(iSlot);
        code.aaload();
        code.checkcast(elementImplDesc);
        code.getfield(elementImplDesc, "segment", CD_MemorySegment);

        // srcOffset = 0L
        code.lconst_0();

        // dst = arr.asSlice(Element$Impl.LAYOUT.byteSize() * i, Element$Impl.LAYOUT.byteSize())
        code.aload(arrSlot);
        code.getstatic(elementImplDesc, "LAYOUT", CD_StructLayout);
        // StructLayout.byteSize() — must call the interface method
        code.invokeinterface(CD_MemoryLayout, "byteSize", MethodTypeDesc.of(CD_long));
        code.iload(iSlot);
        code.i2l();
        code.lmul();
        code.getstatic(elementImplDesc, "LAYOUT", CD_StructLayout);
        code.invokeinterface(CD_MemoryLayout, "byteSize", MethodTypeDesc.of(CD_long));
        code.invokeinterface(CD_MemorySegment, "asSlice", MethodTypeDesc.of(CD_MemorySegment, CD_long, CD_long));

        // dstOffset = 0L
        code.lconst_0();

        // byteCount = Element$Impl.LAYOUT.byteSize()
        code.getstatic(elementImplDesc, "LAYOUT", CD_StructLayout);
        code.invokeinterface(CD_MemoryLayout, "byteSize", MethodTypeDesc.of(CD_long));

        // MemorySegment.copy(src, srcOffset, dst, dstOffset, byteCount)
        code.invokestatic(
            CD_MemorySegment,
            "copy",
            MethodTypeDesc.of(CD_void, CD_MemorySegment, CD_long, CD_MemorySegment, CD_long, CD_long),
            true
        );

        // i++
        code.iinc(iSlot, 1);
        code.goto_(loopStart);
        code.labelBinding(loopEnd);
    }

    // -------------------------------------------------------------------------
    // @FunctionPointer method generation
    // -------------------------------------------------------------------------

    /**
     * Generates the body of a {@code @FunctionPointer} method. The callback interface is converted
     * to a native function pointer (upcall stub) before the downcall, allowing the C library to call
     * back into Java. The {@code Arena} parameter controls the upcall stub's lifetime.
     */
    private static void emitFunctionPointerMethod(
        java.lang.classfile.ClassBuilder cb,
        ClassDesc generatedDesc,
        LibraryModel libraryModel,
        MethodModel.FunctionPointerMethod fp
    ) {
        MethodTypeDesc javaDesc = buildJavaMethodDesc(fp);
        cb.withMethodBody(fp.methodName(), javaDesc, ClassFile.ACC_PUBLIC, code -> {
            int nextLocalSlot = computeLocalBaseSlot(fp);
            boolean needsCallArena = fp.paramTypes().stream().anyMatch(p -> p.type() == NativeType.STRING);
            int callArenaSlot = needsCallArena ? nextLocalSlot++ : -1;
            // cbFdSlot holds the callback FunctionDescriptor; stubSlot holds the upcall MemorySegment.
            // Both come after callArena (if any) in the local variable table.
            int cbFdSlot = nextLocalSlot++;
            int stubSlot = nextLocalSlot++;

            if (needsCallArena) {
                code.invokestatic(CD_Arena, "ofConfined", MethodTypeDesc.of(CD_Arena), true);
                code.astore(callArenaSlot);
            }

            List<NativeType> cbParamTypes = fp.callbackParamTypes()
                .stream()
                .filter(p -> p.type() != NativeType.ARENA)
                .map(p -> nativeLayoutType(p.type()))
                .toList();
            code.trying(tryBlock -> {
                // Build the callback's FunctionDescriptor from the SAM's types
                emitFunctionDescriptor(tryBlock, fp.callbackReturnType(), cbParamTypes);
                tryBlock.astore(cbFdSlot);

                // Get the upcall MethodHandle: LinkerHelper.upcallHandle(MethodHandles.lookup(), CallbackType.class, "sam", cbFd)
                // The caller's lookup is required so LinkerHelper (in a separate module) can resolve
                // a callback interface that's only accessible from the consumer module.
                tryBlock.invokestatic(CD_MethodHandles, "lookup", MethodTypeDesc.of(CD_MethodHandlesLookup));
                ClassDesc callbackDesc = ClassDesc.of(fp.callbackTypeFqn());
                tryBlock.ldc(callbackDesc);
                tryBlock.ldc(fp.callbackMethodName());
                tryBlock.aload(cbFdSlot);
                tryBlock.invokestatic(
                    CD_LinkerHelper,
                    "upcallHandle",
                    MethodTypeDesc.of(CD_MethodHandle, CD_MethodHandlesLookup, CD_Class, CD_String, CD_FunctionDescriptor)
                );

                // Create the stub: LinkerHelper.upcallStub(cbMh, callbackInstance, cbFd, arena)
                int fpParamSlot = computeParamSlot(fp.paramTypes(), findFunctionPointerParamIndex(fp.paramTypes()));
                tryBlock.aload(fpParamSlot);
                tryBlock.aload(cbFdSlot);
                int arenaParamSlot = computeParamSlot(fp.paramTypes(), fp.arenaParamIndex());
                tryBlock.aload(arenaParamSlot);
                tryBlock.invokestatic(
                    CD_LinkerHelper,
                    "upcallStub",
                    MethodTypeDesc.of(CD_MemorySegment, CD_MethodHandle, CD_Object, CD_FunctionDescriptor, CD_Arena)
                );
                tryBlock.astore(stubSlot);

                // Call invokeExact, substituting the pre-computed stub for the FP param
                emitInvokeExactWithStub(tryBlock, generatedDesc, fp, callArenaSlot, stubSlot);

                if (needsCallArena) {
                    tryBlock.aload(callArenaSlot);
                    tryBlock.invokeinterface(CD_Arena, "close", MethodTypeDesc.of(CD_void));
                }
                emitTypedReturn(tryBlock, fp.returnType());
            }, catchBuilder -> catchBuilder.catchingAll(catchBlock -> {
                if (needsCallArena) {
                    int exSlot = stubSlot + 1;
                    catchBlock.astore(exSlot);
                    catchBlock.aload(callArenaSlot);
                    catchBlock.invokeinterface(CD_Arena, "close", MethodTypeDesc.of(CD_void));
                    catchBlock.aload(exSlot);
                }
                catchBlock.new_(CD_AssertionError);
                catchBlock.dup_x1();
                catchBlock.swap();
                catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Throwable));
                catchBlock.athrow();
            }));
        });
    }

    /**
     * Like {@link #emitInvokeExact}, but substitutes the pre-built upcall stub for the
     * callback interface parameter rather than loading it from the local variable slot.
     */
    private static void emitInvokeExactWithStub(
        java.lang.classfile.CodeBuilder cb,
        ClassDesc generatedDesc,
        MethodModel.NativeCallable nm,
        int callArenaSlot,
        int stubSlot
    ) {
        cb.getstatic(generatedDesc, nm.methodName() + "$mh", CD_MethodHandle);

        if (nm.capturesErrno()) {
            cb.getstatic(generatedDesc, "errnoState", CD_MemorySegment);
        }

        int slot = 1;
        for (var param : nm.paramTypes()) {
            if (param.functionPointerTypeName() != null) {
                // Replace the callback interface argument with the upcall stub MemorySegment
                cb.aload(stubSlot);
                slot += 1;
            } else {
                slot += emitLoadParam(cb, param, slot, callArenaSlot);
            }
        }

        MethodTypeDesc invokeExactDesc = buildInvokeExactDesc(nm);
        cb.invokevirtual(CD_MethodHandle, "invokeExact", invokeExactDesc);
    }

    /** Returns the local variable slot of the parameter at {@code paramIndex}, accounting for wide types. */
    private static int computeParamSlot(List<MethodModel.ParamInfo> params, int paramIndex) {
        int slot = 1; // slot 0 = this
        for (int i = 0; i < paramIndex; i++) {
            var type = params.get(i).type();
            slot += (type == NativeType.LONG || type == NativeType.DOUBLE) ? 2 : 1;
        }
        return slot;
    }

    private static int findFunctionPointerParamIndex(List<MethodModel.ParamInfo> params) {
        for (int i = 0; i < params.size(); i++) {
            if (params.get(i).functionPointerTypeName() != null) {
                return i;
            }
        }
        throw new AssertionError("No @FunctionPointer parameter found in param list");
    }

}
