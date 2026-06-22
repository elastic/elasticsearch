/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import static org.objectweb.asm.Opcodes.V21;

/**
 * Extracts public API class files from the {@code java.lang.foreign} package
 * in the running JDK and writes them to a JAR with {@code @PreviewFeature}
 * annotations and the class-file preview flag stripped.
 *
 * <p>The output JAR is intended for use with {@code --patch-module java.base=<jar>}
 * so that modules compiled with {@code --release 21} can reference
 * {@code MemorySegment} and related types without {@code --enable-preview}.
 *
 * <p>This task is registered automatically per-project by
 * {@link ElasticsearchJavaBasePlugin#enableForeignAccess} and must run with
 * JDK 21 as the Gradle JDK (the Foreign Function &amp; Memory API is preview
 * only in JDK 21). The Elasticsearch build uses JDK 21 as its Gradle JDK by
 * default, so no extra configuration is needed.
 *
 * <h3>Inspecting the generated JAR</h3>
 * {@code javap} cannot inspect classes in {@code java.lang.foreign} from a
 * JAR on the classpath because the JDK's module system always resolves them
 * from {@code jrt://java.base} first. To work around this, extract the class
 * file and copy it to a path outside the {@code java/lang/foreign/} directory
 * structure so that {@code javap} reads the file directly:
 * <pre>{@code
 * mkdir -p /tmp/jar-inspect
 * unzip -o -d /tmp/jar-inspect \
 *   libs/native/build/jdk21-foreign-api.jar \
 *   java/lang/foreign/MemorySegment.class
 * cp /tmp/jar-inspect/java/lang/foreign/MemorySegment.class /tmp/jar-inspect/
 * javap -v -p /tmp/jar-inspect/MemorySegment.class
 * }</pre>
 * Key things to verify in the output:
 * <ul>
 *   <li>{@code major version: 65} (Java 21)</li>
 *   <li>{@code minor version: 0} (no preview flag; preview would be 65535)</li>
 *   <li>No {@code @PreviewFeature} in any {@code RuntimeVisibleAnnotations}
 *       or {@code RuntimeInvisibleAnnotations} section (stale constant-pool
 *       string references to {@code PreviewFeature} are harmless)</li>
 * </ul>
 */
@CacheableTask
public abstract class ExtractForeignApiTask extends DefaultTask {

    private static final String PREVIEW_FEATURE_DESCRIPTOR = "Ljdk/internal/javac/PreviewFeature;";
    private static final String FOREIGN_PACKAGE_PREFIX = "java/lang/foreign/";
    private static final int PUBLIC_OR_PROTECTED = Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED;

    @OutputFile
    public abstract RegularFileProperty getOutputJar();

    @TaskAction
    public void extract() throws IOException {
        checkRuntimeJava21();

        Path outputPath = getOutputJar().getAsFile().get().toPath();
        Files.createDirectories(outputPath.getParent());

        FileSystem jrtFs = FileSystems.getFileSystem(URI.create("jrt:/"));
        Path foreignRoot = jrtFs.getPath("modules", "java.base", "java", "lang", "foreign");

        int count = 0;
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(outputPath)); Stream<Path> walk = Files.walk(foreignRoot)) {
            for (Path file : (Iterable<Path>) walk::iterator) {
                if (Files.isRegularFile(file) == false || file.getFileName().toString().endsWith(".class") == false) {
                    continue;
                }
                byte[] stubBytes;
                try (InputStream is = Files.newInputStream(file)) {
                    stubBytes = createStub(is);
                }
                if (stubBytes == null) {
                    continue;
                }
                String entryName = FOREIGN_PACKAGE_PREFIX + file.getFileName().toString();
                JarEntry entry = new JarEntry(entryName);
                entry.setTime(0);
                jar.putNextEntry(entry);
                jar.write(stubBytes);
                jar.closeEntry();
                count++;
                getLogger().lifecycle("  {}", entryName);
            }
        }
        getLogger().lifecycle("Generated {} with {} class(es)", outputPath, count);
    }

    static byte[] createStub(InputStream classStream) throws IOException {
        ClassReader reader = new ClassReader(classStream);
        ClassNode cn = new ClassNode();
        reader.accept(cn, 0);
        checkJava21(cn);

        if ((cn.access & Opcodes.ACC_PUBLIC) == 0) {
            return null;
        }

        cn.version = cn.version & ~Opcodes.V_PREVIEW;

        cn.visibleAnnotations = stripPreviewAnnotations(cn.visibleAnnotations);
        cn.invisibleAnnotations = stripPreviewAnnotations(cn.invisibleAnnotations);

        if (cn.fields != null) {
            cn.fields.removeIf(f -> (f.access & PUBLIC_OR_PROTECTED) == 0);
            for (FieldNode f : cn.fields) {
                f.visibleAnnotations = stripPreviewAnnotations(f.visibleAnnotations);
                f.invisibleAnnotations = stripPreviewAnnotations(f.invisibleAnnotations);
            }
        }
        if (cn.methods != null) {
            cn.methods.removeIf(m -> (m.access & PUBLIC_OR_PROTECTED) == 0);
            for (MethodNode m : cn.methods) {
                m.visibleAnnotations = stripPreviewAnnotations(m.visibleAnnotations);
                m.invisibleAnnotations = stripPreviewAnnotations(m.invisibleAnnotations);
                m.instructions.clear();
                m.tryCatchBlocks.clear();
                if (m.localVariables != null) {
                    m.localVariables.clear();
                }
                m.maxStack = 0;
                m.maxLocals = 0;
            }
        }

        cn.innerClasses.removeIf(ic -> (ic.access & Opcodes.ACC_PUBLIC) == 0);

        ClassWriter writer = new ClassWriter(0);
        cn.accept(writer);
        return writer.toByteArray();
    }

    private static List<AnnotationNode> stripPreviewAnnotations(List<AnnotationNode> anns) {
        if (anns == null) {
            return null;
        }
        anns.removeIf(a -> a.desc.equals(PREVIEW_FEATURE_DESCRIPTOR));
        return anns.isEmpty() ? null : anns;
    }

    private static void checkRuntimeJava21() {
        int jdkVersion = Runtime.version().feature();
        if (jdkVersion != 21) {
            throw new IllegalStateException(
                "extractForeignApi must be run with JDK 21 (found JDK "
                    + jdkVersion
                    + "). "
                    + "The Foreign Function & Memory API is preview in JDK 21 only; on JDK 22+ it is standard and this JAR is unnecessary."
            );
        }
    }

    private static void checkJava21(ClassNode cn) {
        int rawVersion = cn.version & 0xFFFF;
        if (rawVersion != V21) {
            throw new IllegalStateException(
                "Unexpected class file version " + rawVersion + " for " + cn.name + " (expected " + V21 + " / JDK 21)"
            );
        }
    }
}
