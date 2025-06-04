/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HexFormat;
import java.util.Locale;
import java.util.function.Function;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import static org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;
import static org.objectweb.asm.ClassWriter.COMPUTE_MAXS;

public class Utils {

    private static final MessageDigest SHA_256;

    static {
        try {
            SHA_256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private record MismatchInfo(String jarEntryName, String expectedClassSha256, String foundClassSha256) {
        @Override
        public String toString() {
            return "[class='"
                + jarEntryName
                + '\''
                + ", expected='"
                + expectedClassSha256
                + '\''
                + ", found='"
                + foundClassSha256
                + '\''
                + ']';
        }
    }

    public static void patchJar(File inputJar, File outputJar, Collection<PatcherInfo> patchers) {
        patchJar(inputJar, outputJar, patchers, false);
    }

    /**
     * Patches the classes in the input JAR file, using the collection of patchers. Each patcher specifies a target class (its jar entry
     * name) and the SHA256 digest on the class bytes.
     * This digest is checked against the class bytes in the JAR, and if it does not match, an IllegalArgumentException is thrown.
     * If the input file does not contain all the classes to patch specified in the patcher info collection, an IllegalArgumentException
     * is also thrown.
     * @param inputFile the JAR file to patch
     * @param outputFile the output (patched) JAR file
     * @param patchers list of patcher info (classes to patch (jar entry name + optional SHA256 digest) and ASM visitor to transform them)
     * @param unsignJar whether to remove class signatures from the JAR Manifest; set this to true when patching a signed JAR,
     *                  otherwise the patched classes will fail to load at runtime due to mismatched signatures.
     *                  @see <a href="https://docs.oracle.com/javase/tutorial/deployment/jar/intro.html">Understanding Signing and Verification</a>
     */
    public static void patchJar(File inputFile, File outputFile, Collection<PatcherInfo> patchers, boolean unsignJar) {
        var classPatchers = patchers.stream().collect(Collectors.toMap(PatcherInfo::jarEntryName, Function.identity()));
        var mismatchedClasses = new ArrayList<MismatchInfo>();
        try (JarFile jarFile = new JarFile(inputFile); JarOutputStream jos = new JarOutputStream(new FileOutputStream(outputFile))) {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                // Add the entry to the new JAR file
                jos.putNextEntry(new JarEntry(entryName));

                var classPatcher = classPatchers.remove(entryName);
                if (classPatcher != null) {
                    byte[] classToPatch = jarFile.getInputStream(entry).readAllBytes();
                    var classSha256 = SHA_256.digest(classToPatch);

                    if (classPatcher.matches(classSha256)) {
                        ClassReader classReader = new ClassReader(classToPatch);
                        ClassWriter classWriter = new ClassWriter(classReader, COMPUTE_MAXS | COMPUTE_FRAMES);
                        classReader.accept(classPatcher.createVisitor(classWriter), 0);
                        jos.write(classWriter.toByteArray());
                    } else {
                        mismatchedClasses.add(
                            new MismatchInfo(
                                classPatcher.jarEntryName(),
                                HexFormat.of().formatHex(classPatcher.classSha256()),
                                HexFormat.of().formatHex(classSha256)
                            )
                        );
                    }
                } else {
                    try (InputStream is = jarFile.getInputStream(entry)) {
                        if (unsignJar && entryName.equals("META-INF/MANIFEST.MF")) {
                            var manifest = new Manifest(is);
                            for (var manifestEntry: manifest.getEntries().entrySet()) {
                                var nonSignatureAttributes = new Attributes();
                                for (var attribute: manifestEntry.getValue().entrySet()) {
                                    if (attribute.getKey().toString().endsWith("Digest") == false) {
                                        nonSignatureAttributes.put(attribute.getKey(), attribute.getValue());
                                    }
                                }
                                manifestEntry.setValue(nonSignatureAttributes);
                            }
                            manifest.write(jos);
                        } else if (unsignJar == false || entryName.matches("META-INF/.*\\.SF") == false) {
                            // Read the entry's data and write it to the new JAR
                            is.transferTo(jos);
                        }
                    }
                }
                jos.closeEntry();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        if (mismatchedClasses.isEmpty() == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    """
                        Error patching JAR [%s]: SHA256 digest mismatch (%s). This JAR was updated to a version that contains different \
                        classes, for which this patcher was not designed. Please check if the patcher still \
                        applies correctly, and update the SHA256 digest(s).""",
                    inputFile.getName(),
                    mismatchedClasses.stream().map(MismatchInfo::toString).collect(Collectors.joining())
                )
            );
        }

        if (classPatchers.isEmpty() == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "error patching [%s]: the jar does not contain [%s]",
                    inputFile.getName(),
                    String.join(", ", classPatchers.keySet())
                )
            );
        }
    }
}
