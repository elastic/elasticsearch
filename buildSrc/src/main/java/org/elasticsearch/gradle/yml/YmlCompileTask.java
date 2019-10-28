package org.elasticsearch.gradle.yml;

import org.gradle.api.internal.ConventionTask;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskAction;
import shadow.org.objectweb.asm.ClassWriter;
import shadow.org.objectweb.asm.Label;
import shadow.org.objectweb.asm.MethodVisitor;
import shadow.org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class YmlCompileTask extends ConventionTask implements Opcodes {

    private final Property<File> sourceDir;
    private final Property<File> destinationDir;
    private final Property<String> superClass;
    private Saver classSaver = this::saveClass;

    public YmlCompileTask() {
        final JavaPluginConvention javaPlugin = getProject().getConvention().getPlugin(JavaPluginConvention.class);
        final SourceSetContainer sourceSets = javaPlugin.getSourceSets();
        final SourceSet test = sourceSets.findByName("test");
        ObjectFactory objects = getProject().getObjects();
        this.sourceDir = objects.property(File.class).convention(test.getResources().getSrcDirs().iterator().next());
        this.destinationDir = objects.property(File.class).convention(test.getOutput().getClassesDirs().getFiles().iterator().next());
        this.superClass = objects.property(String.class).convention("org/elasticsearch/test/rest/yaml/BaseCompiledYamlTestCase");
    }

    @OutputDirectory
    public File getDestinationDir() {
        return destinationDir.getOrNull();
    }

    public void setDestinationDir(File destinationDir) {
        this.destinationDir.set(destinationDir);
    }

    @InputDirectory
    public File getSourceDir() {
        return sourceDir.getOrNull();
    }

    public void setSourceDir(File sourceDir) {
        this.sourceDir.set(sourceDir);
    }

    @Input
    public String getSuperClass() {
        return superClass.getOrNull().replace('.', '/');
    }

    public void setSuperClass(String superClass) {
        this.superClass.set(superClass);
    }


    @TaskAction
    public void compile() throws IOException {
        for (File f : getProject().fileTree(getSourceDir()).getFiles()) {
            if (f.isFile() && f.getName().endsWith(".yml")) {
                compile(f);
            }
        }
    }

    void compile(File file) throws IOException {
        String path = getSourceDir().toPath().relativize(file.toPath()).toString();
        String className = path.replace(".yml", "_IT");

        ClassWriter cw = new ClassWriter(0);
        MethodVisitor mv;

        cw.visit(V11, ACC_PUBLIC + ACC_SUPER, className, null, getSuperClass(), null);
        cw.visitSource(file.getName(), null);

        int parts = compileInnerClasses(file, className, cw);

        {
            mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, new String[]{"java/io/IOException"});
            mv.visitCode();
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL, getSuperClass(), "<init>", "()V", false);
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }

        {
            mv = cw.visitMethod(ACC_PUBLIC, "getYaml", "()Ljava/lang/String;", null, null);
            mv.visitCode();
            mv.visitTypeInsn(NEW, "java/lang/StringBuilder");
            mv.visitInsn(DUP);
            mv.visitMethodInsn(INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "()V", false);
            mv.visitVarInsn(ASTORE, 1);
            for (int i = 1; i <= parts; i++) {
                mv.visitVarInsn(ALOAD, 1);
                mv.visitMethodInsn(INVOKESTATIC, className + "$BodyPart" + i, "getYaml", "()Ljava/lang/String;", false);
                mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;",
                    false);
                mv.visitInsn(POP);
            }
            mv.visitVarInsn(ALOAD, 1);
            mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;", false);
            mv.visitInsn(ARETURN);
            mv.visitMaxs(2, 2);
            mv.visitEnd();
        }

        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        int lineNumber = 0;
        for (String line : lines) {
            lineNumber++;
            if (line.startsWith("\"")) {
                createTestMethod(cw, className, line.replaceAll("[\":]+", ""), lineNumber);
            }
        }

        cw.visitEnd();

        classSaver.save(className, cw);
    }

    //This method stores entire yml file content inside inner classes to avoid hitting hard limit of 65k bytes for class
    private int compileInnerClasses(File file, String className, ClassWriter cw) throws IOException {
        String body = Files.readString(file.toPath(), StandardCharsets.UTF_8);

        int partNumber = 0;
        for (int i = 0; i < body.length(); i += 64000) {
            partNumber++;
            String bodyPart = body.substring(i, Math.min(i + 64000, body.length()));
            String simpleBodyPartName = "BodyPart" + partNumber;
            String bodyPartName = className + "$" + simpleBodyPartName;
            cw.visitNestMember(bodyPartName);
            cw.visitInnerClass(bodyPartName, className, simpleBodyPartName, ACC_PUBLIC | ACC_FINAL | ACC_STATIC);

            compileInnerClass(simpleBodyPartName, className, bodyPart);
        }

        return partNumber;
    }

    private void compileInnerClass(String simpleBodyPartName, String className, String bodyPart) throws IOException {
        ClassWriter cw = new ClassWriter(0);
        MethodVisitor mv;

        String name = className + "$" + simpleBodyPartName;
        cw.visit(V11, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, name, null, "java/lang/Object", null);
        cw.visitNestHost(className);

        cw.visitInnerClass(name, className, simpleBodyPartName, ACC_PUBLIC | ACC_FINAL | ACC_STATIC);

        {
            mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitCode();
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }
        {
            mv = cw.visitMethod(ACC_PUBLIC | ACC_STATIC, "getYaml", "()Ljava/lang/String;", null, null);
            mv.visitCode();
            mv.visitLdcInsn(bodyPart);
            mv.visitInsn(ARETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }
        cw.visitEnd();

        classSaver.save(name, cw);

    }

    private void createTestMethod(ClassWriter cw, String className, String testName, int lineNumber) {
        MethodVisitor mv;
        mv = cw.visitMethod(ACC_PUBLIC, testName, "()V", null, new String[]{"java/io/IOException"});
        mv.visitAnnotation("Lorg/junit/Test;", true).visitEnd();
        mv.visitCode();
        Label label = new Label();
        mv.visitLabel(label);
        mv.visitLineNumber(lineNumber, label);
        mv.visitVarInsn(ALOAD, 0);
        mv.visitLdcInsn(testName);
        mv.visitMethodInsn(INVOKEVIRTUAL, className, "runTest", "(Ljava/lang/String;)V", false);
        mv.visitInsn(RETURN);
        mv.visitMaxs(2, 1);
        mv.visitEnd();
    }

    private void saveClass(String className, ClassWriter cw) throws IOException {
        Path resolve = getDestinationDir().toPath().resolve(className + ".class");
        Files.createDirectories(resolve.getParent());
        Files.write(resolve, cw.toByteArray());
    }

    void setClassSaver(Saver classSaver) {
        this.classSaver = classSaver;
    }

    public interface Saver {
        void save(String name, ClassWriter cw) throws IOException;
    }
}

