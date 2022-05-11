/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleDescriptor.Version;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.exportsOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.opensOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.providesOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.requiresOf;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;

public class InMemoryModuleFinderTests extends ESTestCase {

    static final Class<UnsupportedOperationException> UOE = UnsupportedOperationException.class;

    public void testOfModuleDescriptor() {
        ModuleDescriptor fooMd = ModuleDescriptor.newModule("foo").build();
        ModuleDescriptor barMd = ModuleDescriptor.newModule("bar").build();
        var finder = InMemoryModuleFinder.of(fooMd, barMd);
        assertThat(finder.findAll().size(), is(2));
        var fooMod = finder.find("foo");
        var barMod = finder.find("bar");
        assertThat(fooMod, isPresent());
        assertThat(barMod, isPresent());
        var fooMref = fooMod.get();
        var barMref = barMod.get();
        assertThat(fooMref.descriptor().name(), is("foo"));
        assertThat(barMref.descriptor().name(), is("bar"));
        expectThrows(UOE, () -> fooMref.open());
        expectThrows(UOE, () -> barMref.open());
        assertThat(fooMref.location(), isPresent());
        assertThat(barMref.location(), isPresent());
    }

    /*
     * Tests exploded jar embedded within an outer jar - how the distro is laid out,
     *   e.g. jar:file:///x/y/impl.jar!/a/b/foo.jar/META-INF/MANIFEST.MF
     *        jar:file:///x/y/impl.jar!/a/b/foo.jar/p/Foo.class
     */
    public void testAutoModuleEmbeddedJar() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Map<String, String> entries = new HashMap<>();
        entries.put("/a/b/foo.jar/META-INF/MANIFEST.MF", "Automatic-Module-Name: foo\n");
        entries.put("/a/b/foo.jar/p/Foo.class", "<empty>");

        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntriesUTF(topLevelDir.resolve("impl.jar"), entries);

        try (FileSystem fileSystem = FileSystems.newFileSystem(outerJar, Map.of(), InMemoryModuleFinderTests.class.getClassLoader())) {
            Path fooRoot = fileSystem.getPath("/a/b/foo.jar");

            // automatic module, and no filtering
            var finder = InMemoryModuleFinder.of(Set.of(), fooRoot);
            assertThat(finder.findAll().size(), is(1));
            var mod = finder.find("foo");
            assertThat(mod, isPresent());
            assertThat(mod.get().descriptor().isAutomatic(), is(true));
        }
    }

    public void testExplicitModuleEmbeddedJar() throws Exception {
        testExplicitModuleEmbeddedJarVersionSpecific(-1); // versionless - module-info in the root
    }

    public void testExplicitModuleEmbeddedJarVersion9() throws Exception {
        testExplicitModuleEmbeddedJarVersionSpecific(9); // META-INF/versions/9/module-info.class
    }

    public void testExplicitModuleEmbeddedJarVersion11() throws Exception {
        testExplicitModuleEmbeddedJarVersionSpecific(11); // META-INF/versions/11/module-info.class
    }

    public void testExplicitModuleEmbeddedJarVersion17() throws Exception {
        testExplicitModuleEmbeddedJarVersionSpecific(17); // META-INF/versions/11/module-info.class
    }

    /*
     * Tests that a module located at a specific versioned entry is correctly located. First
     * generates and compiles the code, then packages it, and lastly locates with the finder.
     *
     * @param version the runtime version number of the module-info entry
     */
    private void testExplicitModuleEmbeddedJarVersionSpecific(int version) throws Exception {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("module-info", "module m { exports p;  opens q; }");
        sources.put("p.Foo", "package p; public class Foo extends q.Bar { }");
        sources.put("q.Bar", "package q; public class Bar { }");
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        String moduleInfoPath = "/a/b/m.jar/module-info.class";
        if (version >= 8) {
            moduleInfoPath = "/a/b/m.jar/META-INF/versions/" + version + "/module-info.class";
        }

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put(moduleInfoPath, classToBytes.get("module-info"));
        jarEntries.put("/a/b/m.jar/p/Foo.class", classToBytes.get("p.Foo"));
        jarEntries.put("/a/b/m.jar/q/Bar.class", classToBytes.get("q.Bar"));
        if (version >= 8) { // locate a bad module-info in the root, to ensure not accessed
            jarEntries.put("/a/b/m.jar/module-info.class", "bad".getBytes(UTF_8));
        }

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(topLevelDir.resolve("impl.jar"), jarEntries);

        try (FileSystem fileSystem = FileSystems.newFileSystem(outerJar, Map.of(), InMemoryModuleFinderTests.class.getClassLoader())) {
            Path mRoot = fileSystem.getPath("/a/b/m.jar");
            var finder = InMemoryModuleFinder.of(Set.of(), mRoot);
            assertThat(finder.findAll().size(), is(1));
            var mref = finder.find("m");
            assertThat(mref, isPresent());
            assertThat(mref.get().descriptor().isAutomatic(), is(false));
            assertThat(mref.get().descriptor().name(), is("m"));
            assertThat(mref.get().descriptor().exports(), containsInAnyOrder(exportsOf("p")));
            assertThat(mref.get().descriptor().opens(), containsInAnyOrder(opensOf("q")));
        }
    }

    /*
     * Tests exploded jar layout on the file system - what we do when running unit tests,
     *   e.g.  file:///x/y/impl/a/b/foo.jar/META-INF/MANIFEST.MF
     *         file:///x/y/impl/a/b/foo.jar/p/Foo.class
     */
    public void testAutoModuleExplodedPath() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path fooRoot = topLevelDir.resolve("a").resolve("b").resolve("foo.jar");
        Files.createDirectories(fooRoot);
        Files.createDirectories(fooRoot.resolve("META-INF"));
        Files.writeString(fooRoot.resolve("META-INF").resolve("MANIFEST.MF"), "Automatic-Module-Name: foo\n");
        Files.createDirectories(fooRoot.resolve("p"));
        Files.writeString(fooRoot.resolve("p").resolve("Foo.class"), "<empty>");

        // automatic module, and no filtering
        var finder = InMemoryModuleFinder.of(Set.of(), fooRoot);
        assertThat(finder.findAll().size(), is(1));
        var mod = finder.find("foo");
        assertThat(mod, isPresent());
        assertThat(mod.get().descriptor().isAutomatic(), is(true));
    }

    public void testExplicitModuleExplodedPath() throws Exception {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("module-info", "module m { exports p;  opens q; }");
        sources.put("p.Foo", "package p; public class Foo extends q.Bar { }");
        sources.put("q.Bar", "package q; public class Bar { }");
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Path topLevelDir = createTempDir(getTestName());
        Path mRoot = topLevelDir.resolve("a").resolve("b").resolve("m.jar");
        Files.createDirectories(mRoot);
        Files.write(mRoot.resolve("module-info.class"), classToBytes.get("module-info"));
        Files.createDirectories(mRoot.resolve("p"));
        Files.write(mRoot.resolve("p").resolve("Foo.class"), classToBytes.get("p.Foo"));
        Files.createDirectories(mRoot.resolve("q"));
        Files.write(mRoot.resolve("q").resolve("Bar.class"), classToBytes.get("q.Bar"));

        // automatic module, and no filtering
        var finder = InMemoryModuleFinder.of(Set.of(), mRoot);
        assertThat(finder.findAll().size(), is(1));
        var mref = finder.find("m");
        assertThat(mref, isPresent());
        assertThat(mref.get().descriptor().isAutomatic(), is(false));
        assertThat(mref.get().descriptor().name(), is("m"));
        assertThat(mref.get().descriptor().exports(), containsInAnyOrder(exportsOf("p")));
        assertThat(mref.get().descriptor().opens(), containsInAnyOrder(opensOf("q")));
    }

    public void testFilterRequiresBasic() {
        final ModuleDescriptor initialMd = ModuleDescriptor.newModule("foo")
            .version("1.0")
            .exports("p")
            .exports("q", Set.of("baz"))
            .requires("bar")
            .requires("baz")
            .opens("open.a")
            .opens("open.b")
            .uses("q.BazService")
            .provides("p.FooService", List.of("q.FooServiceImpl"))
            .packages(Set.of("internal.a", "internal.b"))
            .build();
        {   // empty filter
            var md = InMemoryModuleFinder.filterRequires(initialMd, Set.of());
            assertThat(md, equalTo(initialMd));
        }
        {   // filter a requires that is not present
            var md = InMemoryModuleFinder.filterRequires(initialMd, Set.of("bert"));
            assertThat(md, equalTo(initialMd));
        }
        {   // filter the bar module
            var md = InMemoryModuleFinder.filterRequires(initialMd, Set.of("bar"));
            assertThat(md.name(), is("foo"));
            assertThat(md.version(), isPresent());
            assertThat(md.version().get(), is(Version.parse("1.0")));
            assertThat(md.requires(), hasItem(requiresOf("baz")));
            assertThat(md.requires(), not(hasItem(requiresOf("bar"))));
            assertThat(md.exports(), containsInAnyOrder(exportsOf("p"), exportsOf("q", Set.of("baz"))));
            assertThat(md.opens(), containsInAnyOrder(opensOf("open.a"), opensOf("open.b")));
            assertThat(md.uses(), contains("q.BazService"));
            assertThat(md.provides(), contains(providesOf("p.FooService", List.of("q.FooServiceImpl"))));
            assertThat(md.packages(), containsInAnyOrder("p", "q", "open.a", "open.b", "internal.a", "internal.b"));
        }
    }

    public void testFilterRequiresOpenModule() {
        ModuleDescriptor initialMd = ModuleDescriptor.newOpenModule("openMod").requires("bar").build();
        var md = InMemoryModuleFinder.filterRequires(initialMd, Set.of());
        assertThat(md.isOpen(), is(true));
        assertThat(md, equalTo(initialMd));

        md = InMemoryModuleFinder.filterRequires(initialMd, Set.of("bar"));
        assertThat(md.isOpen(), is(true));
        assertThat(md.name(), equalTo("openMod"));
        assertThat(md.requires(), not(hasItem(requiresOf("bar"))));
        assertThat(md.exports(), iterableWithSize(0));
        assertThat(md.opens(), iterableWithSize(0));
    }

    public void testFilterRequiresAutoModule() {
        ModuleDescriptor initialMd = ModuleDescriptor.newAutomaticModule("autoMod").build();
        var md = InMemoryModuleFinder.filterRequires(initialMd, Set.of());
        assertThat(md.isAutomatic(), is(true));
        assertThat(md, equalTo(initialMd));

        md = InMemoryModuleFinder.filterRequires(initialMd, Set.of("bar"));
        assertThat(md.isAutomatic(), is(true));
        assertThat(md, equalTo(initialMd));
    }
}
