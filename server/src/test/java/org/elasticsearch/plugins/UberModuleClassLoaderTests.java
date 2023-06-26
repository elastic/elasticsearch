/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.junit.After;

import java.io.IOException;
import java.lang.module.Configuration;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESTestCase.WithoutSecurityManager
public class UberModuleClassLoaderTests extends ESTestCase {

    private static Set<URLClassLoader> loaders = new HashSet<>();

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        for (URLClassLoader loader : loaders) {
            loader.close();
        }
        loaders = new HashSet<>();
    }

    /**
     * Test the loadClass method, which is the real entrypoint for users of the classloader
     */
    public void testLoadFromJar() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar.jar");
        createMinimalJar(jar, "p.MyClass");

        try (UberModuleClassLoader loader = getLoader(jar)) {
            {
                Class<?> c = loader.loadClass("p.MyClass");
                assertThat(c, notNullValue());
                Object instance = c.getConstructor().newInstance();
                assertThat(instance.toString(), equalTo("MyClass"));
                assertThat(c.getModule().getName(), equalTo("synthetic"));
            }

            {
                ClassNotFoundException e = expectThrows(ClassNotFoundException.class, () -> loader.loadClass("p.DoesNotExist"));
                assertThat(e.getMessage(), equalTo("p.DoesNotExist"));
            }
        }
    }

    /**
     * Test the findClass method, which we overrode but which will not be called by
     * users of the classloader
     */
    public void testSingleJarFindClass() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        createMinimalJar(jar, "p.MyClass");

        {
            try (UberModuleClassLoader loader = getLoader(jar)) {
                Class<?> c = loader.findClass("p.MyClass");
                assertThat(c, notNullValue());
                c = loader.findClass("p.DoesNotExist");
                assertThat(c, nullValue());
            }
        }

        {
            try (UberModuleClassLoader loader = getLoader(jar)) {
                Class<?> c = loader.findClass("synthetic", "p.MyClass");
                assertThat(c, notNullValue());
                c = loader.findClass("synthetic", "p.DoesNotExist");
                assertThat(c, nullValue());
                c = loader.findClass("does-not-exist", "p.MyClass");
                assertThat(c, nullValue());
                c = loader.findClass(null, "p.MyClass");
                assertThat(c, nullValue());
            }
        }
    }

    public void testSingleJarFindResources() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");

        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + "MyClass", getMinimalSourceString("p", "MyClass", "MyClass"));
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + "MyClass" + ".class", classToBytes.get("p." + "MyClass"));
        jarEntries.put("META-INF/resource.txt", "my resource".getBytes(StandardCharsets.UTF_8));
        JarUtils.createJarWithEntries(jar, jarEntries);

        try (UberModuleClassLoader loader = getLoader(jar)) {

            {
                URL location = loader.findResource("p/MyClass.class");
                assertThat(location, notNullValue());
                location = loader.findResource("p/DoesNotExist.class");
                assertThat(location, nullValue());
                location = loader.findResource("META-INF/resource.txt");
                assertThat(location, notNullValue());
                location = loader.findResource("META-INF/does_not_exist.txt");
                assertThat(location, nullValue());
            }

            {
                URL location = loader.findResource("synthetic", "p/MyClass.class");
                assertThat(location, notNullValue());
                location = loader.findResource("synthetic", "p/DoesNotExist.class");
                assertThat(location, nullValue());
                location = loader.findResource("does-not-exist", "p/MyClass.class");
                assertThat(location, nullValue());
                location = loader.findResource(null, "p/MyClass.class");
                assertThat(location, nullValue());
            }

            {
                Enumeration<URL> locations = loader.findResources("p/MyClass.class");
                assertTrue(locations.hasMoreElements());
                locations = loader.findResources("p/DoesNotExist.class");
                assertFalse(locations.hasMoreElements());
                locations = loader.findResources("META-INF/resource.txt");
                assertTrue(locations.hasMoreElements());
                locations = loader.findResources("META-INF/does_not_exist.txt");
                assertFalse(locations.hasMoreElements());
            }
        }
    }

    public void testHideSplitPackageInParentClassloader() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path overlappingJar = tempDir.resolve("my-split-package.jar");
        createTwoClassJar(overlappingJar, "ParentJarClassInPackageP");

        Path jar = tempDir.resolve("my-jar.jar");
        createMinimalJar(jar, "p.MyClassInPackageP");

        URL[] urls = new URL[] { toUrl(overlappingJar) };

        try (
            URLClassLoader parent = URLClassLoader.newInstance(urls, UberModuleClassLoaderTests.class.getClassLoader());
            UberModuleClassLoader loader = UberModuleClassLoader.getInstance(parent, "synthetic", Set.of(toUrl(jar)))
        ) {
            // stable plugin loader gives us the good class...
            Class<?> c = loader.loadClass("p.MyClassInPackageP");
            Object instance = c.getConstructor().newInstance();
            assertThat(instance.toString(), equalTo("MyClassInPackageP"));

            // but stable plugin loader can't find the class from the split package in the parent loader
            ClassNotFoundException e = expectThrows(ClassNotFoundException.class, () -> loader.loadClass("p.ParentJarClassInPackageP"));
            assertThat(e.getMessage(), equalTo("p.ParentJarClassInPackageP"));

            // we can get the "bad one" from the parent loader
            Class<?> c2 = parent.loadClass("p.ParentJarClassInPackageP");
            Object instance2 = c2.getConstructor().newInstance();
            assertThat(instance2.toString(), containsString("ParentJarClassInPackageP"));

            // stable plugin loader delegates to parent for other packages
            Class<?> c3 = loader.loadClass("q.OtherClass");
            Object instance3 = c3.getConstructor().newInstance();
            assertThat(instance3.toString(), equalTo("OtherClass"));
        }
    }

    public void testNoParentFirstSearch() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path overlappingJar = tempDir.resolve("my-overlapping-jar.jar");
        String className = "MyClass";
        createTwoClassJar(overlappingJar, className);

        Path jar = tempDir.resolve("my-jar.jar");
        createMinimalJar(jar, "p." + className);

        URL[] urls = new URL[] { toUrl(overlappingJar) };

        try (
            URLClassLoader parent = URLClassLoader.newInstance(urls, UberModuleClassLoaderTests.class.getClassLoader());
            UberModuleClassLoader loader = UberModuleClassLoader.getInstance(parent, "synthetic", Set.of(toUrl(jar)))
        ) {
            // stable plugin loader gives us the good class...
            Class<?> c = loader.loadClass("p.MyClass");
            Object instance = c.getConstructor().newInstance();
            assertThat(instance.toString(), equalTo("MyClass"));

            // we can get the "bad one" from the parent loader
            Class<?> c2 = parent.loadClass("p.MyClass");
            Object instance2 = c2.getConstructor().newInstance();
            assertThat(instance2.toString(), equalTo("MyClass[From the two-class jar]"));

            // stable plugin loader delegates to parent for other packages
            Class<?> c3 = loader.loadClass("q.OtherClass");
            Object instance3 = c3.getConstructor().newInstance();
            assertThat(instance3.toString(), equalTo("OtherClass"));
        }
    }

    public void testMultipleJarSinglePackageLoadClass() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path jar1 = tempDir.resolve("my-jar-1.jar");
        Path jar2 = tempDir.resolve("my-jar-2.jar");
        Path jar3 = tempDir.resolve("my-jar-3.jar");

        createMinimalJar(jar1, "p.FirstClass");
        createMinimalJar(jar2, "p.SecondClass");
        createMinimalJar(jar3, "p.ThirdClass");

        try (UberModuleClassLoader loader = getLoader(List.of(jar1, jar2, jar3))) {
            Class<?> c1 = loader.loadClass("p.FirstClass");
            Object instance1 = c1.getConstructor().newInstance();
            assertThat(instance1.toString(), equalTo("FirstClass"));

            Class<?> c2 = loader.loadClass("p.SecondClass");
            Object instance2 = c2.getConstructor().newInstance();
            assertThat(instance2.toString(), equalTo("SecondClass"));

            Class<?> c3 = loader.loadClass("p.ThirdClass");
            Object instance3 = c3.getConstructor().newInstance();
            assertThat(instance3.toString(), equalTo("ThirdClass"));
        }
    }

    public void testSplitPackageJarLoadClass() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path jar1 = tempDir.resolve("my-jar-1.jar");
        Path jar2 = tempDir.resolve("my-jar-2.jar");
        Path jar3 = tempDir.resolve("my-jar-3.jar");

        createMinimalJar(jar1, "p.a.FirstClass");
        createMinimalJar(jar2, "p.split.SecondClass");
        createMinimalJar(jar3, "p.split.ThirdClass");

        try (UberModuleClassLoader loader = getLoader(List.of(jar1, jar2, jar3))) {
            Class<?> c1 = loader.loadClass("p.a.FirstClass");
            Object instance1 = c1.getConstructor().newInstance();
            assertThat(instance1.toString(), equalTo("FirstClass"));

            Class<?> c2 = loader.loadClass("p.split.SecondClass");
            Object instance2 = c2.getConstructor().newInstance();
            assertThat(instance2.toString(), equalTo("SecondClass"));

            Class<?> c3 = loader.loadClass("p.split.ThirdClass");
            Object instance3 = c3.getConstructor().newInstance();
            assertThat(instance3.toString(), equalTo("ThirdClass"));
        }
    }

    public void testPackagePerJarLoadClass() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path jar1 = tempDir.resolve("my-jar-1.jar");
        Path jar2 = tempDir.resolve("my-jar-2.jar");
        Path jar3 = tempDir.resolve("my-jar-3.jar");

        createMinimalJar(jar1, "p.a.FirstClass");
        createMinimalJar(jar2, "p.b.SecondClass");
        createMinimalJar(jar3, "p.c.ThirdClass");

        try (UberModuleClassLoader loader = getLoader(List.of(jar1, jar2, jar3))) {
            Class<?> c1 = loader.loadClass("p.a.FirstClass");
            Object instance1 = c1.getConstructor().newInstance();
            assertThat(instance1.toString(), equalTo("FirstClass"));

            Class<?> c2 = loader.loadClass("p.b.SecondClass");
            Object instance2 = c2.getConstructor().newInstance();
            assertThat(instance2.toString(), equalTo("SecondClass"));

            Class<?> c3 = loader.loadClass("p.c.ThirdClass");
            Object instance3 = c3.getConstructor().newInstance();
            assertThat(instance3.toString(), equalTo("ThirdClass"));
        }
    }

    public void testModuleDenyList() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        createSingleClassJar(jar, "p.MyImportingClass", """
            package p;
            import java.sql.ResultSet;
            public class MyImportingClass {
                @Override
                public String toString() {
                    return "MyImportingClass[imports " + ResultSet.class.getSimpleName() + "]";
                }
            }
            """);

        try (
            UberModuleClassLoader denyListLoader = UberModuleClassLoader.getInstance(
                UberModuleClassLoaderTests.class.getClassLoader(),
                ModuleLayer.boot(),
                "synthetic",
                Set.of(toUrl(jar)),
                Set.of("java.sql", "java.sql.rowset") // if present, java.sql.rowset requires java.sql transitively
            )
        ) {
            Class<?> denyListed = denyListLoader.loadClass("p.MyImportingClass");
            assertThat(denyListed, notNullValue());
            Object instance1 = denyListed.getConstructor().newInstance();
            IllegalAccessError e = expectThrows(IllegalAccessError.class, instance1::toString);
            assertThat(e.getMessage(), containsString("cannot access class java.sql.ResultSet (in module java.sql)"));
        }

        try (UberModuleClassLoader unrestrictedLoader = getLoader(jar)) {
            Class<?> unrestricted = unrestrictedLoader.loadClass("p.MyImportingClass");
            assertThat(unrestricted, notNullValue());
            Object instance2 = unrestricted.getConstructor().newInstance();
            assertThat(instance2.toString(), containsString("MyImportingClass[imports ResultSet]"));
        }
    }

    // multi-release jar handling is delegated to the internal URLClassLoader
    public void testMultiReleaseJar() throws Exception {
        Object fooBar = newFooBar(true, 7, 9, 11, 15);
        assertThat(fooBar.toString(), equalTo("FooBar 15"));

        fooBar = newFooBar(false);
        assertThat(fooBar.toString(), equalTo("FooBar 0"));

        fooBar = newFooBar(true, 10_000);
        assertThat(fooBar.toString(), equalTo("FooBar 0"));
    }

    public void testServiceLoadingWithMetaInf() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-service-jar.jar");

        createServiceTestSingleJar(jar, false, true);

        try (
            UberModuleClassLoader cl = UberModuleClassLoader.getInstance(
                this.getClass().getClassLoader(),
                "p.synthetic.test",
                Set.of(toUrl(jar))
            )
        ) {
            Class<?> demoClass = cl.loadClass("p.ServiceCaller");
            var method = demoClass.getMethod("demo");
            String result = (String) method.invoke(null);
            assertThat(result, equalTo("The test string."));
        }
    }

    public void testServiceLoadingWithModuleInfo() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-service-jar.jar");

        createServiceTestSingleJar(jar, true, false);

        try (
            UberModuleClassLoader cl = UberModuleClassLoader.getInstance(
                this.getClass().getClassLoader(),
                "p.synthetic.test",
                Set.of(toUrl(jar))
            )
        ) {
            Class<?> demoClass = cl.loadClass("p.ServiceCaller");
            var method = demoClass.getMethod("demo");
            String result = (String) method.invoke(null);
            assertThat(result, equalTo("The test string."));
        }
    }

    public void testServiceLoadingWithRedundantDeclarations() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-service-jar.jar");

        createServiceTestSingleJar(jar, true, true);

        try (
            UberModuleClassLoader cl = UberModuleClassLoader.getInstance(
                this.getClass().getClassLoader(),
                "p.synthetic.test",
                Set.of(toUrl(jar))
            )
        ) {
            Class<?> demoClass = cl.loadClass("p.ServiceCaller");
            var method = demoClass.getMethod("demo");
            String result = (String) method.invoke(null);
            assertThat(result, equalTo("The test string."));
        }
    }

    private static void createServiceTestSingleJar(Path jar, boolean modularize, boolean addMetaInfService) throws IOException {
        String serviceInterface = """
            package p;

            public interface MyService {
                public String getTestString();
            }
            """;
        String implementingClass = """
            package p;

            public class MyServiceImpl implements MyService {

                public MyServiceImpl() {
                    // no-args
                }

                @Override
                public String getTestString() {
                    return "The test string.";
                }
            }
            """;
        String retrievingClass = """
            package p;

            import java.util.ServiceLoader;
            import java.util.random.RandomGenerator;

            public class ServiceCaller {
                public static String demo() {
                    // check no error if we load a service from the jdk
                    ServiceLoader<RandomGenerator> randomLoader = ServiceLoader.load(RandomGenerator.class);

                    ServiceLoader<MyService> loader = ServiceLoader.load(MyService.class, ServiceCaller.class.getClassLoader());
                    return loader.findFirst().get().getTestString();
                }
            }
            """;
        String moduleInfo = """
            module p.services {
                uses p.MyService;
                provides p.MyService with p.MyServiceImpl;
            }
            """;

        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p.MyService", serviceInterface);
        sources.put("p.MyServiceImpl", implementingClass);
        sources.put("p.ServiceCaller", retrievingClass);
        if (modularize) {
            sources.put("module-info", moduleInfo);
        }
        var compiledCode = InMemoryJavaCompiler.compile(sources);
        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/MyService.class", compiledCode.get("p.MyService"));
        jarEntries.put("p/MyServiceImpl.class", compiledCode.get("p.MyServiceImpl"));
        jarEntries.put("p/ServiceCaller.class", compiledCode.get("p.ServiceCaller"));
        if (modularize) {
            jarEntries.put("module-info.class", compiledCode.get("module-info"));
        }
        if (addMetaInfService) {
            jarEntries.put("META-INF/services/p.MyService", "p.MyServiceImpl".getBytes(StandardCharsets.UTF_8));
        }

        JarUtils.createJarWithEntries(jar, jarEntries);
    }

    public void testServiceLoadingWithOptionalDependencies() throws Exception {
        try (UberModuleClassLoader loader = getServiceTestLoader(true)) {

            // check module descriptor
            ModuleDescriptor synthetic = loader.getLayer().findModule("synthetic").orElseThrow().getDescriptor();

            assertThat(synthetic.uses(), equalTo(Set.of(
                "p.required.LetterService",
                "p.optional.AnimalService",
                "q.jar.one.NumberService",
                "q.jar.two.FooBarService"
            )));
            // the descriptor model uses a list ordering that we don't guarantee, so we convert the provider list to maps and sets
            Map<String, Set<String>> serviceProviders =
                synthetic.provides().stream().collect(Collectors.toMap(ModuleDescriptor.Provides::service,
                    provides -> new HashSet<>(provides.providers())));
            assertThat(serviceProviders, equalTo(Map.of(
                "p.required.LetterService", Set.of("q.jar.one.JarOneProvider", "q.jar.two.JarTwoProvider"),
                // optional dependencies found and added
                "p.optional.AnimalService", Set.of("q.jar.one.JarOneOptionalProvider", "q.jar.two.JarTwoOptionalProvider"),
                "q.jar.one.NumberService", Set.of("q.jar.one.JarOneProvider", "q.jar.two.JarTwoProvider"),
                "q.jar.two.FooBarService", Set.of("q.jar.two.JarTwoProvider")
            )));

            // Now let's make sure the module system lets us load available services
            Class<?> serviceCallerClass = loader.loadClass("q.caller.ServiceCaller");
            Object instance = serviceCallerClass.getConstructor().newInstance();

            var requiredParent = serviceCallerClass.getMethod("callServiceFromRequiredParent");
            assertThat(requiredParent.invoke(instance), equalTo("AB"));
            var optionalParent = serviceCallerClass.getMethod("callServiceFromOptionalParent");
            assertThat(optionalParent.invoke(instance), equalTo("catdog")); // our service provider worked
            var modular = serviceCallerClass.getMethod("callServiceFromModularJar");
            assertThat(modular.invoke(instance), equalTo("12"));
            var nonModular = serviceCallerClass.getMethod("callServiceFromNonModularJar");
            assertThat(nonModular.invoke(instance), equalTo("foo"));
        }
    }

    public void testServiceLoadingWithoutOptionalDependencies() throws Exception {
        try (UberModuleClassLoader loader = getServiceTestLoader(false)) {

            // check module descriptor
            ModuleDescriptor synthetic = loader.getLayer().findModule("synthetic").orElseThrow().getDescriptor();
            assertThat(synthetic.uses(), equalTo(Set.of(
                "p.required.LetterService",
                "q.jar.one.NumberService",
                "q.jar.two.FooBarService"
            )));
            // the descriptor model uses a list ordering that we don't guarantee, so we convert the provider list to maps and sets
            Map<String, Set<String>> serviceProviders =
                synthetic.provides().stream().collect(Collectors.toMap(ModuleDescriptor.Provides::service,
                    provides -> new HashSet<>(provides.providers())));
            assertThat(serviceProviders, equalTo(Map.of(
                "p.required.LetterService", Set.of("q.jar.one.JarOneProvider", "q.jar.two.JarTwoProvider"),
                "q.jar.one.NumberService", Set.of("q.jar.one.JarOneProvider", "q.jar.two.JarTwoProvider"),
                "q.jar.two.FooBarService", Set.of("q.jar.two.JarTwoProvider")
            )));

            // Now let's make sure the module system lets us load available services
            Class<?> serviceCallerClass = loader.loadClass("q.caller.ServiceCaller");
            Object instance = serviceCallerClass.getConstructor().newInstance();

            var requiredParent = serviceCallerClass.getMethod("callServiceFromRequiredParent");
            assertThat(requiredParent.invoke(instance), equalTo("AB"));
            var optionalParent = serviceCallerClass.getMethod("callServiceFromOptionalParent");
            // service not found at runtime, so we don't try to load the provider
            assertThat(optionalParent.invoke(instance), equalTo("Optional AnimalService dependency not present at runtime."));
            var modular = serviceCallerClass.getMethod("callServiceFromModularJar");
            assertThat(modular.invoke(instance), equalTo("12"));
            var nonModular = serviceCallerClass.getMethod("callServiceFromNonModularJar");
            assertThat(nonModular.invoke(instance), equalTo("foo"));
        }
    }

    /*
     * A class in our ubermodule may use SPI to load a service. Our test scenario needs to work out the following four
     * conditions:
     *
     * 1. Service defined in package exported in parent layer.
     * 2. Service defined in a compile-time dependency, optionally present at runtime
     * 3. Service defined in modular jar in uberjar
     * 4. Service defined in non-modular jar in uberjar
     *
     * In all these cases, our ubermodule should declare that it uses each service *available at runtime*, and that
     * it provides these services with the correct providers.
     *
     * We create a jar for each scenario, plus "service caller" jar with a demo class, then
     * create an UberModuleClassLoader for the relevant jars.
     */
    private static UberModuleClassLoader getServiceTestLoader(boolean includeOptionalDeps) throws IOException {
        Path libDir = createTempDir("libs");
        Path parentJar = createRequiredJarForParentLayer(libDir);
        Path optionalJar = createOptionalJarForParentLayer(libDir);

        Set<String> moduleNames = includeOptionalDeps ? Set.of("p.required", "p.optional") : Set.of("p.required");
        ModuleFinder parentModuleFinder = includeOptionalDeps ? ModuleFinder.of(parentJar, optionalJar) : ModuleFinder.of(parentJar);
        Configuration parentLayerConfiguration = ModuleLayer.boot()
            .configuration()
            .resolve(parentModuleFinder, ModuleFinder.of(), moduleNames);

        URLClassLoader parentLoader = new URLClassLoader(new URL[]{pathToUrlUnchecked(parentJar)});
        loaders.add(parentLoader);
        URLClassLoader optionalLoader = new URLClassLoader(new URL[]{pathToUrlUnchecked(optionalJar)}, parentLoader);
        loaders.add(optionalLoader);
        ModuleLayer parentLayer = ModuleLayer.defineModules(
            parentLayerConfiguration,
            List.of(ModuleLayer.boot()),
            (String moduleName) -> {
                if (moduleName.equals("p.required")) {
                    return parentLoader;
                } else if (includeOptionalDeps && moduleName.equals("p.optional")) {
                    return optionalLoader;
                }
                return null;
            }).layer();

        // jars for the ubermodule
        Path modularJar = createModularizedJarForBundle(libDir);
        Path nonModularJar = createNonModularizedJarForBundle(libDir, parentJar, optionalJar, modularJar);
        Path serviceCallerJar = createServiceCallingJarForBundle(libDir, parentJar, optionalJar, modularJar, nonModularJar);

        Set<Path> jarPaths = new HashSet<>(Set.of(modularJar, nonModularJar, serviceCallerJar));
        return UberModuleClassLoader.getInstance(
            parentLayer.findLoader(includeOptionalDeps ? "p.optional" : "p.required"),
            parentLayer,
            "synthetic",
            jarPaths.stream().map(UberModuleClassLoaderTests::pathToUrlUnchecked).collect(Collectors.toSet()),
            Set.of()
        );
    }

    private static Path createServiceCallingJarForBundle(Path libDir, Path parentJar, Path optionalJar, Path modularJar, Path nonModularJar)
        throws IOException {

        String serviceCaller = """
            package q.caller;

            import p.optional.AnimalService;
            import p.required.LetterService;
            import q.jar.one.NumberService;
            import q.jar.two.FooBarService;

            import java.util.ServiceLoader;
            import java.util.stream.Collectors;

            public class ServiceCaller {

                public ServiceCaller() { }

                public String callServiceFromRequiredParent() {
                    return ServiceLoader.load(LetterService.class, ServiceCaller.class.getClassLoader()).stream()
                            .map(ServiceLoader.Provider::get)
                            .map(LetterService::getLetter)
                            .sorted()
                            .collect(Collectors.joining());
                }

                public String callServiceFromOptionalParent() {
                    Class<?> animalServiceClass;
                    try {
                        animalServiceClass = ServiceCaller.class.getClassLoader().loadClass("p.optional.AnimalService");
                    } catch (ClassNotFoundException e) {
                        return "Optional AnimalService dependency not present at runtime.";
                    }
                    return ServiceLoader.load(animalServiceClass, ServiceCaller.class.getClassLoader()).stream()
                            .map(ServiceLoader.Provider::get)
                            .filter(instance -> instance instanceof AnimalService)
                            .map(AnimalService.class::cast)
                            .map(AnimalService::getAnimal)
                            .sorted()
                            .collect(Collectors.joining());
                }

                public String callServiceFromModularJar() {
                    return ServiceLoader.load(NumberService.class, ServiceCaller.class.getClassLoader()).stream()
                            .map(ServiceLoader.Provider::get)
                            .map(NumberService::getNumber)
                            .sorted()
                            .collect(Collectors.joining());
                }

                public String callServiceFromNonModularJar() {
                    return ServiceLoader.load(FooBarService.class, ServiceCaller.class.getClassLoader()).stream()
                            .map(ServiceLoader.Provider::get)
                            .map(FooBarService::getFoo)
                            .sorted()
                            .collect(Collectors.joining());
                }
            }
            """;

        Map<String, CharSequence> serviceCallerJarSources = new HashMap<>();
        serviceCallerJarSources.put("q.caller.ServiceCaller", serviceCaller);
        var serviceCallerJarCompiled = InMemoryJavaCompiler.compile(
            serviceCallerJarSources,
            "--class-path",
            String.join(
                System.getProperty("path.separator"),
                parentJar.toString(),
                optionalJar.toString(),
                modularJar.toString(),
                nonModularJar.toString()
            )
        );

        assertThat(serviceCallerJarCompiled, notNullValue());

        Path serviceCallerJar = libDir.resolve("service-caller.jar");
        JarUtils.createJarWithEntries(
            serviceCallerJar,
            serviceCallerJarCompiled.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().replace(".", "/") + ".class", Map.Entry::getValue))
        );
        return serviceCallerJar;
    }

    private static Path createNonModularizedJarForBundle(Path libDir, Path parentJar, Path optionalJar, Path modularJar)
        throws IOException {
        String serviceFromNonModularJar = """
            package q.jar.two;
            public interface FooBarService {
                String getFoo();
            }
            """;
        String providerInNonModularJar = """
            package q.jar.two;
            import q.jar.one.NumberService;
            import p.required.LetterService;

            public class JarTwoProvider implements FooBarService, LetterService, NumberService {
                @Override public String getFoo() { return "foo"; }
                @Override public String getLetter() { return "B"; }
                @Override public String getNumber() { return "2"; }
            }
            """;
        String providerOfOptionalInNonModularJar = """
            package q.jar.two;
            import p.optional.AnimalService;

            public class JarTwoOptionalProvider implements AnimalService {
                @Override public String getAnimal() { return "cat"; }
            }

            """;

        Map<String, CharSequence> nonModularJarSources = new HashMap<>();
        nonModularJarSources.put("q.jar.two.FooBarService", serviceFromNonModularJar);
        nonModularJarSources.put("q.jar.two.JarTwoProvider", providerInNonModularJar);
        nonModularJarSources.put("q.jar.two.JarTwoOptionalProvider", providerOfOptionalInNonModularJar);
        var nonModularJarCompiled = InMemoryJavaCompiler.compile(
            nonModularJarSources,
            "--class-path",
            String.join(System.getProperty("path.separator"), parentJar.toString(), optionalJar.toString(), modularJar.toString())
        );

        assertThat(nonModularJarCompiled, notNullValue());

        Path nonModularJar = libDir.resolve("non-modular.jar");
        var nonModularJarEntries = new HashMap<String, byte[]>();
        nonModularJarEntries.put("q/jar/two/FooBarService.class", nonModularJarCompiled.get("q.jar.two.FooBarService"));
        nonModularJarEntries.put("q/jar/two/JarTwoProvider.class", nonModularJarCompiled.get("q.jar.two.JarTwoProvider"));
        nonModularJarEntries.put("q/jar/two/JarTwoOptionalProvider.class", nonModularJarCompiled.get("q.jar.two.JarTwoOptionalProvider"));
        nonModularJarEntries.put("META-INF/services/p.required.LetterService", "q.jar.two.JarTwoProvider".getBytes(StandardCharsets.UTF_8));
        nonModularJarEntries.put(
            "META-INF/services/p.optional.AnimalService",
            "q.jar.two.JarTwoOptionalProvider".getBytes(StandardCharsets.UTF_8)
        );
        nonModularJarEntries.put("META-INF/services/q.jar.one.NumberService", "q.jar.two.JarTwoProvider".getBytes(StandardCharsets.UTF_8));
        nonModularJarEntries.put("META-INF/services/q.jar.two.FooBarService", "q.jar.two.JarTwoProvider".getBytes(StandardCharsets.UTF_8));
        JarUtils.createJarWithEntries(nonModularJar, nonModularJarEntries);
        return nonModularJar;
    }

    private static Path createModularizedJarForBundle(Path libDir) throws IOException {
        String serviceFromModularJar = """
            package q.jar.one;
            public interface NumberService {
                String getNumber();
            }
            """;
        String providerInModularJar = """
            package q.jar.one;
            import p.required.LetterService;

            public class JarOneProvider implements LetterService, NumberService {
                @Override public String getLetter() { return "A"; }
                @Override public String getNumber() { return "1"; }
            }
            """;
        String providerOfOptionalInModularJar = """
            package q.jar.one;
            import p.optional.AnimalService;

            public class JarOneOptionalProvider implements AnimalService {
                @Override public String getAnimal() { return "dog"; }
            }
            """;
        String moduleInfo = """
            module q.jar.one {
                exports q.jar.one;

                requires p.required;
                requires static p.optional;

                provides p.optional.AnimalService with q.jar.one.JarOneOptionalProvider;
                provides p.required.LetterService with q.jar.one.JarOneProvider;
                provides q.jar.one.NumberService with q.jar.one.JarOneProvider;
            }
            """;

        Map<String, CharSequence> modularizedJarSources = new HashMap<>();
        modularizedJarSources.put("q.jar.one.NumberService", serviceFromModularJar);
        modularizedJarSources.put("q.jar.one.JarOneProvider", providerInModularJar);
        modularizedJarSources.put("q.jar.one.JarOneOptionalProvider", providerOfOptionalInModularJar);
        modularizedJarSources.put("module-info", moduleInfo);
        var modularJarCompiled = InMemoryJavaCompiler.compile(modularizedJarSources, "--module-path", libDir.toString());

        assertThat(modularJarCompiled, notNullValue());

        Path modularJar = libDir.resolve("modular.jar");
        JarUtils.createJarWithEntries(
            modularJar,
            modularJarCompiled.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().replace(".", "/") + ".class", Map.Entry::getValue))
        );
        return modularJar;
    }

    private static Path createOptionalJarForParentLayer(Path libDir) throws IOException {
        String serviceFromOptionalParent = """
            package p.optional;
            public interface AnimalService {
                String getAnimal();
            }
            """;
        String optionalParentModuleInfo = """
            module p.optional { exports p.optional; }
            """;
        Map<String, CharSequence> optionalModuleSources = new HashMap<>();
        optionalModuleSources.put("p.optional.AnimalService", serviceFromOptionalParent);
        optionalModuleSources.put("module-info", optionalParentModuleInfo);
        var optionalModuleCompiled = InMemoryJavaCompiler.compile(optionalModuleSources);
        assertThat(optionalModuleCompiled, notNullValue());

        Path optionalJar = libDir.resolve("optional.jar");
        JarUtils.createJarWithEntries(
            optionalJar,
            optionalModuleCompiled.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().replace(".", "/") + ".class", Map.Entry::getValue))
        );
        return optionalJar;
    }

    private static Path createRequiredJarForParentLayer(Path libDir) throws IOException {
        String serviceFromRequiredParent = """
            package p.required;
            public interface LetterService {
                String getLetter();
            }
            """;
        String requiredParentModuleInfo = """
            module p.required { exports p.required; }
            """;
        Map<String, CharSequence> requiredModuleSources = new HashMap<>();
        requiredModuleSources.put("p.required.LetterService", serviceFromRequiredParent);
        requiredModuleSources.put("module-info", requiredParentModuleInfo);
        var requiredModuleCompiled = InMemoryJavaCompiler.compile(requiredModuleSources);

        assertThat(requiredModuleCompiled, notNullValue());
        Path parentJar = libDir.resolve("parent.jar");

        JarUtils.createJarWithEntries(
            parentJar,
            requiredModuleCompiled.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().replace(".", "/") + ".class", Map.Entry::getValue))
        );
        return parentJar;
    }

    private static UberModuleClassLoader getLoader(Path jar) {
        return getLoader(List.of(jar));
    }

    private static UberModuleClassLoader getLoader(List<Path> jars) {
        return UberModuleClassLoader.getInstance(
            UberModuleClassLoaderTests.class.getClassLoader(),
            "synthetic",
            jars.stream().map(UberModuleClassLoaderTests::pathToUrlUnchecked).collect(Collectors.toSet())
        );
    }

    private static URL pathToUrlUnchecked(Path path) {
        try {
            return toUrl(path);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /*
     * Instantiates an instance of FooBar. First generates and compiles the code, then packages it,
     * and lastly loads the FooBar class with an embedded loader.
     *
     * @param enableMulti whether to set the multi-release attribute
     * @param versions the runtime version number of the entries to create in the jar
     */
    private Object newFooBar(boolean enableMulti, int... versions) throws Exception {
        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/FooBar.class", classBytesForVersion(0)); // root version is always 0
        if (enableMulti) {
            jarEntries.put("META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(StandardCharsets.UTF_8));
        }
        stream(versions).forEach(v -> jarEntries.put("META-INF/versions/" + v + "/p/FooBar.class", classBytesForVersion(v)));

        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar.jar");
        JarUtils.createJarWithEntries(jar, jarEntries);
        Class<?> c;
        try (UberModuleClassLoader loader = getLoader(jar)) {
            c = loader.loadClass("p.FooBar");
        }
        return c.getConstructor().newInstance();
    }

    /* Creates a FooBar class that reports the given version in its toString. */
    static byte[] classBytesForVersion(int version) {
        return InMemoryJavaCompiler.compile("p.FooBar", String.format(Locale.ENGLISH, """
            package p;
            public class FooBar {
                @Override public String toString() {
                    return "FooBar %d";
                }
            }
            """, version));
    }

    private static void createMinimalJar(Path jar, String className) throws Exception {
        if (className.contains(".") == false) {
            createSingleClassJar(jar, className, getMinimalSourceString("", className, className));
            return;
        }
        int lastIndex = className.lastIndexOf(".");
        String simpleClassName = className.substring(lastIndex + 1);
        String packageName = className.substring(0, lastIndex);
        createSingleClassJar(jar, className, getMinimalSourceString(packageName, simpleClassName, simpleClassName));
    }

    private static void createSingleClassJar(Path jar, String canonicalName, String source) throws IOException {
        Map<String, CharSequence> sources = new HashMap<>();
        String jarPath = canonicalName.replace(".", "/") + ".class";
        sources.put(canonicalName, source);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put(jarPath, classToBytes.get(canonicalName));
        JarUtils.createJarWithEntries(jar, jarEntries);
    }

    // returns a jar with two classes, one in package p with name $className, and one called q.OtherClass
    private static void createTwoClassJar(Path jar, String className) throws IOException {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + className, getMinimalSourceString("p", className, className + "[From the two-class jar]"));
        sources.put("q.OtherClass", getMinimalSourceString("q", "OtherClass", "OtherClass"));
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + className + ".class", classToBytes.get("p." + className));
        jarEntries.put("q/OtherClass.class", classToBytes.get("q.OtherClass"));
        JarUtils.createJarWithEntries(jar, jarEntries);
    }

    // returns a class that overrides the toString method
    private static String getMinimalSourceString(String packageName, String className, String toStringOutput) {
        return String.format(Locale.ENGLISH, """
            %s
            public class %s {
                @Override
                public String toString() {
                    return "%s";
                }
            }
            """, Strings.isNullOrEmpty(packageName) ? "" : "package " + packageName + ";", className, toStringOutput);
    }

    private static URL toUrl(Path jar) throws MalformedURLException {
        return jar.toUri().toURL();
    }
}
