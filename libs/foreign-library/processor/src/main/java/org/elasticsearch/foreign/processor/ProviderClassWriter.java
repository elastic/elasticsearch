/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.LibraryProvider;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.processor.model.LibraryModel;

import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassSignature;
import java.lang.classfile.Label;
import java.lang.classfile.MethodSignature;
import java.lang.classfile.Signature.ClassTypeSig;
import java.lang.classfile.Signature.TypeArg;
import java.lang.classfile.attribute.SignatureAttribute;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;

import javax.annotation.processing.Filer;
import javax.lang.model.element.TypeElement;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Object;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;

/**
 * Generates the {@code <InterfaceName>$Provider} class for each {@code @LibrarySpecification}-annotated
 * interface. The provider implements {@code LibraryProvider}, is public so it can be registered as an
 * SPI service, and instantiates the package-private {@code $Impl} (which only this class can reach,
 * since they share a package).
 */
class ProviderClassWriter {

    private static final ClassDesc CD_Class = ClassDesc.of("java.lang.Class");
    private static final ClassDesc CD_LibraryProvider = ClassDesc.of(LibraryProvider.class.getName());
    private static final ClassDesc CD_Platform = ClassDesc.of(Platform.class.getName());
    private static final MethodTypeDesc MTD_Platform_current = MethodTypeDesc.of(CD_Platform);

    private final Filer filer;
    private final int classFileVersion;

    ProviderClassWriter(Filer filer, int classFileVersion) {
        this.filer = filer;
        this.classFileVersion = classFileVersion;
    }

    /**
     * Generates and writes the {@code $Provider} class for the given library model.
     */
    void generate(LibraryModel model, TypeElement sourceElement) throws Exception {
        ClassDesc implDesc = ClassDesc.of(model.implQualifiedName());
        ClassDesc interfaceDesc = ClassDesc.of(model.qualifiedName());
        String providerName = model.providerQualifiedName();
        ClassDesc providerDesc = ClassDesc.of(providerName);

        byte[] classBytes = ClassFile.of().build(providerDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.PUBLIC, AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);
            cb.withInterfaceSymbols(CD_LibraryProvider);
            cb.with(
                SignatureAttribute.of(
                    ClassSignature.of(
                        ClassTypeSig.of(CD_Object),
                        ClassTypeSig.of(CD_LibraryProvider, TypeArg.of(ClassTypeSig.of(interfaceDesc)))
                    )
                )
            );

            // public no-arg constructor (required for SPI)
            cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, init -> {
                init.aload(0);
                init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                init.return_();
            });

            // public Class<T> libraryClass() { return <Interface>.class; }
            cb.withMethod("libraryClass", MethodTypeDesc.of(CD_Class), ClassFile.ACC_PUBLIC, mb -> {
                mb.with(SignatureAttribute.of(MethodSignature.of(ClassTypeSig.of(CD_Class, TypeArg.of(ClassTypeSig.of(interfaceDesc))))));
                mb.withCode(code -> {
                    code.ldc(interfaceDesc);
                    code.areturn();
                });
            });

            // public T load() { ... }
            cb.withMethod("load", MethodTypeDesc.of(CD_Object), ClassFile.ACC_PUBLIC, mb -> {
                mb.with(SignatureAttribute.of(MethodSignature.of(ClassTypeSig.of(interfaceDesc))));
                mb.withCode(code -> {
                    if (model.unavailableOn().isEmpty() == false) {
                        // Platform p = Platform.current();
                        code.invokestatic(CD_Platform, "current", MTD_Platform_current);
                        code.astore(1);
                        for (String platformName : model.unavailableOn()) {
                            // if (p == Platform.<platformName>) return null;
                            Label skip = code.newLabel();
                            code.aload(1);
                            code.getstatic(CD_Platform, platformName, CD_Platform);
                            code.if_acmpne(skip);
                            code.aconst_null();
                            code.areturn();
                            code.labelBinding(skip);
                        }
                    }
                    // return new $Impl();
                    code.new_(implDesc);
                    code.dup();
                    code.invokespecial(implDesc, "<init>", MethodTypeDesc.of(CD_void));
                    code.areturn();
                });
            });
        });

        try (var os = filer.createClassFile(providerName, sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }
}
