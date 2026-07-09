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

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayoutArray;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegment;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegmentAdapter;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Object;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_String;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.fieldClassDesc;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.primitiveClassDesc;

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
    private static final ClassDesc CD_Arena = ClassDesc.of("java.lang.foreign.Arena");
    private static final ClassDesc CD_StructLayout = ClassDesc.of("java.lang.foreign.StructLayout");
    private static final ClassDesc CD_VarHandle = ClassDesc.of("java.lang.invoke.VarHandle");
    private static final ClassDesc CD_MemoryLayoutPathElement = ClassDesc.of("java.lang.foreign.MemoryLayout$PathElement");
    private static final ClassDesc CD_MemoryLayoutPathElementArray = ClassDesc.ofDescriptor(
        "[Ljava/lang/foreign/MemoryLayout$PathElement;"
    );
    private static final ClassDesc CD_LinkerHelper = ClassDesc.of("org.elasticsearch.foreign.LinkerHelper");
    private static final ClassDesc CD_LinkerAdapter = ClassDesc.of("org.elasticsearch.foreign.adapter.LinkerAdapter");
    private static final ClassDesc CD_LoaderHelper = ClassDesc.of("org.elasticsearch.foreign.LoaderHelper");
    private static final ClassDesc CD_Addressable = ClassDesc.of("org.elasticsearch.foreign.Addressable");

    private static final MethodTypeDesc MTD_FunctionDescriptor_ofVoid = MethodTypeDesc.of(CD_FunctionDescriptor, CD_MemoryLayoutArray);
    private static final MethodTypeDesc MTD_FunctionDescriptor_of = MethodTypeDesc.of(
        CD_FunctionDescriptor,
        CD_MemoryLayout,
        CD_MemoryLayoutArray
    );
    private static final MethodTypeDesc MTD_downcallHandle = MethodTypeDesc.of(
        CD_MethodHandle,
        CD_String,
        CD_FunctionDescriptor,
        CD_LinkerOptionArray
    );
    private static final MethodTypeDesc MTD_adaptCritical = MethodTypeDesc.of(
        CD_MethodHandle,
        CD_Lookup,
        CD_MethodHandle,
        CD_Class,
        CD_String
    );
    private static final MethodTypeDesc MTD_MemorySegmentAdapter_getString = MethodTypeDesc.of(CD_String, CD_MemorySegment, CD_long);
    private static final MethodTypeDesc MTD_Arena_ofConfined = MethodTypeDesc.of(CD_Arena);
    private static final MethodTypeDesc MTD_Arena_ofAuto = MethodTypeDesc.of(CD_Arena);
    private static final MethodTypeDesc MTD_Arena_close = MethodTypeDesc.of(CD_void);
    private static final MethodTypeDesc MTD_MemorySegmentAdapter_allocateString = MethodTypeDesc.of(CD_MemorySegment, CD_Arena, CD_String);
    private static final MethodTypeDesc MTD_structLayout = MethodTypeDesc.of(CD_StructLayout, CD_MemoryLayoutArray);
    private static final MethodTypeDesc MTD_paddingLayout = MethodTypeDesc.of(CD_MemoryLayout, CD_long);
    private static final MethodTypeDesc MTD_withName = MethodTypeDesc.of(CD_MemoryLayout, CD_String);
    private static final MethodTypeDesc MTD_byteOffset = MethodTypeDesc.of(CD_long, CD_MemoryLayoutPathElementArray);
    private static final MethodTypeDesc MTD_groupElement = MethodTypeDesc.of(CD_MemoryLayoutPathElement, CD_String);
    private static final MethodTypeDesc MTD_varHandle = MethodTypeDesc.of(CD_VarHandle, CD_MemoryLayoutPathElementArray);
    private static final MethodTypeDesc MTD_varHandleWithoutOffset = MethodTypeDesc.of(
        CD_VarHandle,
        CD_MemoryLayout,
        CD_MemoryLayoutPathElement
    );
    private static final MethodTypeDesc MTD_allocate_layout = MethodTypeDesc.of(CD_MemorySegment, CD_MemoryLayout);
    private static final MethodTypeDesc MTD_allocate_layout_count = MethodTypeDesc.of(CD_MemorySegment, CD_MemoryLayout, CD_long);
    private static final MethodTypeDesc MTD_byteSize = MethodTypeDesc.of(CD_long);
    private static final MethodTypeDesc MTD_byteAlignment = MethodTypeDesc.of(CD_long);
    private static final MethodTypeDesc MTD_critical = MethodTypeDesc.of(CD_LinkerOptionArray);
    private static final MethodTypeDesc MTD_asSlice = MethodTypeDesc.of(CD_MemorySegment, CD_long, CD_long);

    private final Filer filer;
    private final int classFileVersion;

    ImplClassWriter(Filer filer, int classFileVersion) {
        this.filer = filer;
        this.classFileVersion = classFileVersion;
    }

    /** Generates and writes the {@code $Impl} class for the given library model, plus struct companion classes. */
    void generate(LibraryModel model, TypeElement sourceElement) throws Exception {
        // Generate $Pack for record structs and $Impl for interface structs
        for (StructModel struct : model.structs()) {
            if (struct.isRecord()) {
                generatePackClass(model, struct, sourceElement);
            } else {
                generateStructImplClass(model, struct, sourceElement);
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
                    emitMhFieldInit(clinit, generatedDesc, nm);
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
    // Struct $Pack class generation (for @StructSpecification records)
    // -------------------------------------------------------------------------

    /**
     * Generates a {@code $Pack} companion class for a {@code @StructSpecification} record.
     * The class exposes {@code LAYOUT}, per-field offsets, per-field VarHandles (used by
     * {@code $Impl} readers), and a {@code pack(record, segment, baseOffset)} method.
     */
    private void generatePackClass(LibraryModel model, StructModel struct, TypeElement sourceElement) throws Exception {
        String packQualifiedName = model.packageName().isEmpty()
            ? model.simpleName() + "$" + struct.simpleName() + "$Pack"
            : model.packageName() + "." + model.simpleName() + "$" + struct.simpleName() + "$Pack";
        ClassDesc packDesc = ClassDesc.of(packQualifiedName);

        // The record type in the enclosing interface
        String recordQualifiedName = model.packageName().isEmpty()
            ? model.simpleName() + "$" + struct.simpleName()
            : model.packageName() + "." + model.simpleName() + "$" + struct.simpleName();
        ClassDesc recordDesc = ClassDesc.of(recordQualifiedName);

        List<FieldModel> fields = struct.fields();
        List<LayoutField> layout = computeLayout(fields);

        byte[] classBytes = ClassFile.of().build(packDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);

            // LAYOUT field: static final StructLayout (package-private for use by $Impl readers)
            cb.withField("LAYOUT", CD_StructLayout, fb -> fb.withFlags(AccessFlag.STATIC, AccessFlag.FINAL));

            // One offset field per component (private — used only by pack())
            for (FieldModel field : fields) {
                cb.withField(
                    field.name() + "$offset",
                    CD_long,
                    fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL)
                );
            }

            // One VarHandle field per component (package-private — used by $Impl to read elements)
            for (FieldModel field : fields) {
                cb.withField(field.name() + "$vh", CD_VarHandle, fb -> fb.withFlags(AccessFlag.STATIC, AccessFlag.FINAL));
            }

            // <clinit>: initialize LAYOUT, offsets, and VarHandles
            cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
                // LAYOUT = MemoryLayout.structLayout(...)
                emitStructLayoutArray(clinit, layout);
                clinit.invokestatic(CD_MemoryLayout, "structLayout", MTD_structLayout, true);
                clinit.putstatic(packDesc, "LAYOUT", CD_StructLayout);

                // Initialize offset for each field
                for (FieldModel field : fields) {
                    clinit.getstatic(packDesc, "LAYOUT", CD_StructLayout);
                    clinit.loadConstant(1);
                    clinit.anewarray(CD_MemoryLayoutPathElement);
                    clinit.dup();
                    clinit.loadConstant(0);
                    clinit.ldc(field.name());
                    clinit.invokestatic(CD_MemoryLayoutPathElement, "groupElement", MTD_groupElement, true);
                    clinit.aastore();
                    clinit.invokeinterface(CD_MemoryLayout, "byteOffset", MTD_byteOffset);
                    clinit.putstatic(packDesc, field.name() + "$offset", CD_long);
                }

                // Initialize VarHandle for each field
                for (FieldModel field : fields) {
                    clinit.getstatic(packDesc, "LAYOUT", CD_StructLayout);
                    clinit.ldc(field.name());
                    clinit.invokestatic(CD_MemoryLayoutPathElement, "groupElement", MTD_groupElement, true);
                    clinit.invokestatic(CD_MemorySegmentAdapter, "varHandleWithoutOffset", MTD_varHandleWithoutOffset);
                    clinit.putstatic(packDesc, field.name() + "$vh", CD_VarHandle);
                }

                clinit.return_();
            });

            // Private no-arg constructor
            cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PRIVATE, init -> {
                init.aload(0);
                init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                init.return_();
            });

            // pack(RecordType src, MemorySegment dest, long baseOffset)
            List<ClassDesc> packParamDescs = List.of(recordDesc, CD_MemorySegment, CD_long);
            MethodTypeDesc packMethodDesc = MethodTypeDesc.of(CD_void, packParamDescs);
            // static package-private: no access flags = package-private
            cb.withMethodBody("pack", packMethodDesc, ClassFile.ACC_STATIC, pack -> {
                // slot 0 = src, slot 1 = dest, slot 2 = baseOffset (long, takes 2 slots)
                for (FieldModel field : fields) {
                    // dest.set(ValueLayout.JAVA_XXX, baseOffset + <name>$offset, src.<name>())
                    pack.aload(1); // dest
                    emitValueLayout(pack, field.type().layoutType());
                    pack.lload(2); // baseOffset
                    pack.getstatic(packDesc, field.name() + "$offset", CD_long);
                    pack.ladd();
                    pack.aload(0); // src
                    // invoke the record accessor
                    pack.invokevirtual(recordDesc, field.name(), MethodTypeDesc.of(fieldClassDesc(field.type())));

                    // MemorySegment.set(ValueLayout, long, value)
                    ClassDesc valueLayoutDesc = valueLayoutClassDesc(field.type());
                    ClassDesc fieldJavaDesc = fieldClassDesc(field.type());
                    MethodTypeDesc setDesc = MethodTypeDesc.of(CD_void, valueLayoutDesc, CD_long, fieldJavaDesc);
                    pack.invokeinterface(CD_MemorySegment, "set", setDesc);
                }
                pack.return_();
            });
        });

        try (var os = filer.createClassFile(packQualifiedName, sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    // -------------------------------------------------------------------------
    // Struct $Impl class generation (for @StructSpecification interfaces)
    // -------------------------------------------------------------------------

    /**
     * Generates a {@code $Impl} class for a {@code @StructSpecification} interface. The class
     * implements the interface and {@code Addressable}, wraps a single {@link
     * java.lang.foreign.MemorySegment}, and exposes VarHandle-backed accessors for scalar fields
     * and indexed accessors for {@code @ArrayField} methods.
     */
    private void generateStructImplClass(LibraryModel model, StructModel struct, TypeElement sourceElement) throws Exception {
        String structImplQualifiedName = model.packageName().isEmpty()
            ? model.simpleName() + "$" + struct.simpleName() + "$Impl"
            : model.packageName() + "." + model.simpleName() + "$" + struct.simpleName() + "$Impl";
        ClassDesc structImplDesc = ClassDesc.of(structImplQualifiedName);

        String structInterfaceQualifiedName = model.packageName().isEmpty()
            ? model.simpleName() + "$" + struct.simpleName()
            : model.packageName() + "." + model.simpleName() + "$" + struct.simpleName();
        ClassDesc structInterfaceDesc = ClassDesc.of(structInterfaceQualifiedName);
        ClassDesc addressableDesc = ClassDesc.of("org.elasticsearch.foreign.Addressable");

        List<FieldModel> fields = struct.fields();
        List<LayoutField> layout = computeLayout(fields);
        String packPrefix = model.packageName().isEmpty() ? model.simpleName() : model.packageName() + "." + model.simpleName();

        byte[] classBytes = ClassFile.of().build(structImplDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);
            cb.withInterfaceSymbols(structInterfaceDesc, addressableDesc);

            // static final StructLayout LAYOUT
            cb.withField("LAYOUT", CD_StructLayout, fb -> fb.withFlags(AccessFlag.STATIC, AccessFlag.FINAL));

            // One VarHandle per field: scalar fields use "name$vh", array pointer fields use "name$ptr$vh"
            for (FieldModel field : fields) {
                String vhName = field.isArray() ? field.name() + "$ptr$vh" : field.name() + "$vh";
                cb.withField(vhName, CD_VarHandle, fb -> fb.withFlags(AccessFlag.STATIC, AccessFlag.FINAL));
            }

            // final MemorySegment segment
            cb.withField("segment", CD_MemorySegment, fb -> fb.withFlags(AccessFlag.FINAL));

            // <clinit>: initialize LAYOUT and every VarHandle
            cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
                emitStructLayoutArray(clinit, layout);
                clinit.invokestatic(CD_MemoryLayout, "structLayout", MTD_structLayout, true);
                clinit.putstatic(structImplDesc, "LAYOUT", CD_StructLayout);

                for (FieldModel field : fields) {
                    String vhName = field.isArray() ? field.name() + "$ptr$vh" : field.name() + "$vh";
                    clinit.getstatic(structImplDesc, "LAYOUT", CD_StructLayout);
                    clinit.ldc(field.name());
                    clinit.invokestatic(CD_MemoryLayoutPathElement, "groupElement", MTD_groupElement, true);
                    clinit.invokestatic(CD_MemorySegmentAdapter, "varHandleWithoutOffset", MTD_varHandleWithoutOffset);
                    clinit.putstatic(structImplDesc, vhName, CD_VarHandle);
                }

                clinit.return_();
            });

            // Package-private no-arg constructor: this.segment = Arena.ofAuto().allocate(LAYOUT)
            cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), 0, init -> {
                init.aload(0);
                init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                init.aload(0);
                init.invokestatic(CD_Arena, "ofAuto", MTD_Arena_ofAuto, true);
                init.getstatic(structImplDesc, "LAYOUT", CD_StructLayout);
                init.invokeinterface(CD_Arena, "allocate", MTD_allocate_layout);
                init.putfield(structImplDesc, "segment", CD_MemorySegment);
                init.return_();
            });

            // public MemorySegment segment() { return segment; }
            cb.withMethodBody("segment", MethodTypeDesc.of(CD_MemorySegment), ClassFile.ACC_PUBLIC, seg -> {
                seg.aload(0);
                seg.getfield(structImplDesc, "segment", CD_MemorySegment);
                seg.areturn();
            });

            // Accessor methods for every field
            for (FieldModel field : fields) {
                if (field.isArray()) {
                    List<FieldModel> elementFields = resolveElementFields(model, field.elementSimpleName());
                    emitArrayFieldGetter(cb, structImplDesc, packPrefix, field, elementFields);
                } else {
                    emitScalarFieldGetter(cb, structImplDesc, field);
                }
            }
        });

        try (var os = filer.createClassFile(structImplQualifiedName, sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    /** Emits a scalar-field accessor: {@code return (<type>) name$vh.get(segment);}. */
    private static void emitScalarFieldGetter(ClassBuilder cb, ClassDesc structImplDesc, FieldModel field) {
        ClassDesc returnDesc = fieldClassDesc(field.type());
        MethodTypeDesc methodDesc = MethodTypeDesc.of(returnDesc);
        cb.withMethodBody(field.name(), methodDesc, ClassFile.ACC_PUBLIC, code -> {
            code.getstatic(structImplDesc, field.name() + "$vh", CD_VarHandle);
            code.aload(0);
            code.getfield(structImplDesc, "segment", CD_MemorySegment);
            code.invokevirtual(CD_VarHandle, "get", MethodTypeDesc.of(returnDesc, CD_MemorySegment));
            emitTypedReturnScalar(code, field.type());
        });
    }

    /**
     * Emits an {@code @ArrayField} accessor. Loads the pointer via {@code name$ptr$vh}, slices to
     * the requested element, reads each of the element record's fields via the element's
     * {@code $Pack} VarHandles, then constructs and returns the record.
     */
    private static void emitArrayFieldGetter(
        ClassBuilder cb,
        ClassDesc structImplDesc,
        String packPrefix,
        FieldModel arrayField,
        List<FieldModel> elementFields
    ) {
        ClassDesc elementRecordDesc = ClassDesc.of(packPrefix + "$" + arrayField.elementSimpleName());
        ClassDesc elementPackDesc = ClassDesc.of(packPrefix + "$" + arrayField.elementSimpleName() + "$Pack");
        MethodTypeDesc methodDesc = MethodTypeDesc.of(elementRecordDesc, ClassDesc.ofDescriptor("I"));

        cb.withMethodBody(arrayField.name(), methodDesc, ClassFile.ACC_PUBLIC, code -> {
            // slot 0 = this, slot 1 = index (int)
            // MemorySegment ptr = (MemorySegment) name$ptr$vh.get(segment)
            code.getstatic(structImplDesc, arrayField.name() + "$ptr$vh", CD_VarHandle);
            code.aload(0);
            code.getfield(structImplDesc, "segment", CD_MemorySegment);
            code.invokevirtual(CD_VarHandle, "get", MethodTypeDesc.of(CD_MemorySegment, CD_MemorySegment));
            code.astore(2);

            // long elementSize = ElementPack.LAYOUT.byteSize()
            code.getstatic(elementPackDesc, "LAYOUT", CD_StructLayout);
            code.invokeinterface(CD_MemoryLayout, "byteSize", MTD_byteSize);
            code.lstore(3);

            // MemorySegment elementSeg = ptr.asSlice(index * elementSize, elementSize)
            code.aload(2); // ptr
            code.iload(1); // index
            code.i2l();
            code.lload(3); // elementSize
            code.lmul();
            code.lload(3); // elementSize
            code.invokeinterface(CD_MemorySegment, "asSlice", MTD_asSlice);
            code.astore(5);

            // new ElementRecord(field0, field1, ...) - read each field via ElementPack.<name>$vh
            code.new_(elementRecordDesc);
            code.dup();
            List<ClassDesc> ctorParams = new ArrayList<>();
            for (FieldModel ef : elementFields) {
                ClassDesc efDesc = fieldClassDesc(ef.type());
                code.getstatic(elementPackDesc, ef.name() + "$vh", CD_VarHandle);
                code.aload(5); // elementSeg
                code.invokevirtual(CD_VarHandle, "get", MethodTypeDesc.of(efDesc, CD_MemorySegment));
                ctorParams.add(efDesc);
            }
            code.invokespecial(elementRecordDesc, "<init>", MethodTypeDesc.of(CD_void, ctorParams));
            code.areturn();
        });
    }

    /** Looks up the field list of a nested struct in the same library by simple name. */
    private static List<FieldModel> resolveElementFields(LibraryModel model, String simpleName) {
        for (StructModel s : model.structs()) {
            if (s.simpleName().equals(simpleName)) {
                return s.fields();
            }
        }
        throw new AssertionError("no struct model for element type: " + simpleName);
    }

    /** Emits the return instruction for a scalar {@link NativeType}. */
    private static void emitTypedReturnScalar(CodeBuilder cb, NativeType type) {
        switch (type) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.ireturn();
            case LONG -> cb.lreturn();
            case FLOAT -> cb.freturn();
            case DOUBLE -> cb.dreturn();
            case ADDRESS -> cb.areturn();
            case VOID, STRING -> throw new AssertionError("unexpected scalar field type: " + type);
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
     * Resolves the native symbol and stores the resulting {@code MethodHandle} in the static
     * {@code <name>$mh} field. Handles {@code @CaptureErrno} and {@code @Variadic} options.
     */
    private static void emitMhFieldInit(CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        boolean hasFallbackAdapter = nm.fallbackAdapterClassName() != null;

        if (hasFallbackAdapter) {
            cb.invokestatic(CD_MethodHandles, "lookup", MethodTypeDesc.of(CD_Lookup));
        }

        cb.ldc(nm.cSymbol());
        emitFunctionDescriptor(cb, nm.returnType(), nm.paramTypes());
        emitLinkerOptions(cb, nm);

        if (nm.capturesErrno()) {
            cb.invokestatic(CD_LinkerHelper, "downcallHandleWithErrno", MTD_downcallHandle);
        } else {
            cb.invokestatic(CD_LinkerHelper, "downcallHandle", MTD_downcallHandle);
        }

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

    private static void emitLinkerOptions(CodeBuilder cb, MethodModel nm) {
        if (nm.isCritical()) {
            cb.invokestatic(CD_LinkerAdapter, "critical", MTD_critical);
        } else if (nm.firstVariadicArg() >= 0) {
            // @Variadic (with or without @CaptureErrno): supply firstVariadicArg option.
            // When @CaptureErrno is also present, downcallHandleWithErrno prepends captureCallState internally.
            cb.loadConstant(1);
            cb.anewarray(CD_LinkerOption);
            cb.dup();
            cb.loadConstant(0);
            cb.loadConstant(nm.firstVariadicArg());
            // Linker.Option is an interface, so invokestatic must mark isInterface=true
            cb.invokestatic(CD_LinkerOption, "firstVariadicArg", MethodTypeDesc.of(CD_LinkerOption, ClassDesc.ofDescriptor("I")), true);
            cb.aastore();
        } else {
            // No special options
            cb.iconst_0();
            cb.anewarray(CD_LinkerOption);
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
        FieldModel arrayField = targetStruct.fields()
            .stream()
            .filter(FieldModel::isArray)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Struct " + nm.structReturnSimpleName() + " has no @ArrayField"));
        FieldModel lengthField = targetStruct.fields()
            .stream()
            .filter(f -> f.name().equals(arrayField.lengthFieldName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing length field " + arrayField.lengthFieldName()));
        NativeType countType = lengthField.type();

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

            // arr = Arena.ofAuto().allocate(ElemPack.LAYOUT, (long) elements.length)
            code.invokestatic(CD_Arena, "ofAuto", MTD_Arena_ofAuto, true);
            code.getstatic(packDesc, "LAYOUT", CD_StructLayout);
            code.aload(1);
            code.arraylength();
            code.i2l();
            code.invokeinterface(CD_Arena, "allocate", MTD_allocate_layout_count);
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
            code.getstatic(structImplDesc, lengthField.name() + "$vh", CD_VarHandle);
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
            code.getstatic(structImplDesc, arrayField.name() + "$ptr$vh", CD_VarHandle);
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

    // -------------------------------------------------------------------------
    // Layout computation (C natural alignment)
    // -------------------------------------------------------------------------

    /** A field along with the padding (in bytes) that precedes it in the struct layout. */
    private record LayoutField(FieldModel field, long paddingBefore) {}

    /**
     * Computes per-field padding using C natural-alignment rules, assuming a 64-bit ADDRESS
     * (8 bytes). Every field is aligned to its own size; padding is inserted before any field
     * whose alignment isn't satisfied by the running offset.
     */
    private static List<LayoutField> computeLayout(List<FieldModel> fields) {
        List<LayoutField> result = new ArrayList<>();
        long offset = 0;
        for (FieldModel field : fields) {
            long align = byteAlignment(field.type());
            long padding = (offset % align == 0) ? 0 : (align - offset % align);
            result.add(new LayoutField(field, padding));
            offset += padding + byteSize(field.type());
        }
        return result;
    }

    private static long byteSize(NativeType type) {
        return switch (type) {
            case BOOLEAN, BYTE -> 1;
            case SHORT -> 2;
            case INT, FLOAT -> 4;
            case LONG, DOUBLE, ADDRESS -> 8;
            case VOID, STRING, ADDRESSABLE -> throw new AssertionError("no size for type: " + type);
        };
    }

    private static long byteAlignment(NativeType type) {
        return byteSize(type);
    }

    /**
     * Emits bytecode that constructs the {@code MemoryLayout[]} array for
     * {@code MemoryLayout.structLayout(...)}, including named field layouts and any inline
     * padding layouts. The array is left on the operand stack.
     */
    private static void emitStructLayoutArray(CodeBuilder cb, List<LayoutField> layout) {
        int arraySize = layout.size();
        for (LayoutField lf : layout) {
            if (lf.paddingBefore() > 0) {
                arraySize++;
            }
        }
        cb.loadConstant(arraySize);
        cb.anewarray(CD_MemoryLayout);
        int arrayIndex = 0;
        for (LayoutField lf : layout) {
            if (lf.paddingBefore() > 0) {
                cb.dup();
                cb.loadConstant(arrayIndex++);
                cb.loadConstant(lf.paddingBefore());
                cb.invokestatic(CD_MemoryLayout, "paddingLayout", MTD_paddingLayout, true);
                cb.aastore();
            }
            cb.dup();
            cb.loadConstant(arrayIndex++);
            emitValueLayout(cb, lf.field().type());
            cb.ldc(lf.field().name());
            cb.invokeinterface(CD_MemoryLayout, "withName", MTD_withName);
            cb.aastore();
        }
    }

    /** Returns the specific ValueLayout subtype class descriptor for a field type. */
    private static ClassDesc valueLayoutClassDesc(NativeType type) {
        return switch (type) {
            case INT -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfInt;");
            case LONG -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfLong;");
            case SHORT -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfShort;");
            case BYTE -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfByte;");
            case BOOLEAN -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfBoolean;");
            case FLOAT -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfFloat;");
            case DOUBLE -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfDouble;");
            case ADDRESS, STRING -> ClassDesc.ofDescriptor("Ljava/lang/foreign/AddressLayout;");
            case ADDRESSABLE -> throw new AssertionError("ADDRESSABLE cannot be a struct field type");
            case VOID -> throw new AssertionError("void cannot be a field type");
        };
    }
}
