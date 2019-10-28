package org.elasticsearch.gradle.yml;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Test;
import shadow.org.objectweb.asm.ClassWriter;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class YmlCompileTaskTests extends GradleUnitTestCase {

    public static abstract class Base {
        Set<String> tests = new HashSet<>();

        protected abstract String getYaml();

        public void runTest(String name) {
            tests.add(name);
        }
    }

    public static abstract class LineNumbers extends Base {

        @Override
        public void runTest(String name) {
            throw new IllegalArgumentException();
        }

        protected abstract void multiTest1();

        protected abstract void multiTest2();
    }

    public static final class AsmClassLoader extends ClassLoader {

        Map<String, byte[]> compiledClasses = new HashMap<>();

        private void addClass(String name, ClassWriter cw) {
            compiledClasses.put(name.replace('/', '.'), cw.toByteArray());
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            if (compiledClasses.containsKey(name) == false) {
                throw new ClassNotFoundException();
            }
            byte[] bytes = compiledClasses.get(name);
            return defineClass(name, bytes, 0, bytes.length);
        }
    }

    public void testSimple() throws Exception {
        check("simple", "simple test");
    }

    public void testMultiple() throws Exception {
        check("multiple", "multiTest1", "multiTest2");
    }

    public void testLong() throws Exception {
        check("long", "long test");
    }

    public void testLineNumber1() throws Exception {
        try {
            LineNumbers obj = compile("multiple", LineNumbers.class);
            obj.multiTest1();
            fail();
        } catch (IllegalArgumentException e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            assertEquals("multiple.yml", stackTrace[1].getFileName());
            assertEquals(2, stackTrace[1].getLineNumber());
        }
    }

    public void testLineNumber2() throws Exception {
        try {
            LineNumbers obj = compile("multiple", LineNumbers.class);
            obj.multiTest2();
            fail();
        } catch (IllegalArgumentException e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            assertEquals("multiple.yml", stackTrace[1].getFileName());
            assertEquals(8, stackTrace[1].getLineNumber());
        }
    }

    private void check(String fileName, String... tests) throws Exception {
        Base obj = compile(fileName, Base.class);
        checkYml(fileName, obj);
        for (String test : tests) {
            Method method = obj.getClass().getMethod(test);
            assertNotNull(method.getAnnotation(Test.class));
            method.invoke(obj);
            assertTrue(obj.tests.contains(test));
        }
    }

    private <T extends Base> T compile(String name, Class<T> baseClass) throws Exception {
        Path testYmlPath = getTestYmlPath(name);
        File file = testYmlPath.toFile();
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply("java");
        YmlCompileTask ymlCompileTask = (YmlCompileTask) project.task(Map.of(Task.TASK_TYPE, YmlCompileTask.class), "ymlCompile");
        ymlCompileTask.setSuperClass(baseClass.getName());
        ymlCompileTask.setSourceDir(file.getParentFile().getParentFile());
        AsmClassLoader asmClassLoader = new AsmClassLoader();
        ymlCompileTask.setClassSaver(asmClassLoader::addClass);
        ymlCompileTask.compile(file);
        return baseClass.cast(Class.forName("yml." + name + "_IT", true, asmClassLoader).getConstructor().newInstance());
    }

    private <T extends Base> void checkYml(String name, Base obj) throws Exception {
        String yml = Files.readString(getTestYmlPath(name), StandardCharsets.UTF_8);
        assertEquals(yml, obj.getYaml());
    }

    private Path getTestYmlPath(String name) throws URISyntaxException {
        return Path.of(YmlCompileTaskTests.class.getResource(name + ".yml").toURI());
    }
}
