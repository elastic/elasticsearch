package org.elasticsearch.gradle.internal.rewrite;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.net.URI;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Provides access to Rewrite classes resolved and loaded from the supplied dependency configuration.
 * This keeps them isolated from the rest of Gradle's runtime classpath.
 * So there shouldn't be problems with conflicting transitive dependency versions or anything like that.
 */
@SuppressWarnings({"unchecked", "UnusedReturnValue", "InnerClassMayBeStatic"})
public class RewriteReflectiveFacade {

    public RewriteReflectiveFacade() {
    }

    private ClassLoader getClassLoader() {
        return getClass().getClassLoader();
    }

    private List<RewriteReflectiveFacade.SourceFile> parseBase(Object real, Iterable<Path> sourcePaths, Path baseDir, RewriteReflectiveFacade.InMemoryExecutionContext ctx) {
        try {
            Class<?> executionContextClass = getClass().getClassLoader().loadClass("org.openrewrite.ExecutionContext");
            List<Object> results = (List<Object>) real.getClass()
                    .getMethod("parse", Iterable.class, Path.class, executionContextClass)
                    .invoke(real, sourcePaths, baseDir, ctx.real);
            return results.stream()
                    .map(RewriteReflectiveFacade.SourceFile::new)
                    .collect(toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class EnvironmentBuilder {
        private final Object real;

        private EnvironmentBuilder(Object real) {
            this.real = real;
        }

        public RewriteReflectiveFacade.EnvironmentBuilder scanRuntimeClasspath(String... acceptPackages) {
            try {
                real.getClass().getMethod("scanRuntimeClasspath", String[].class).invoke(real, new Object[]{acceptPackages});
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public RewriteReflectiveFacade.EnvironmentBuilder scanJar(Path jar, ClassLoader classLoader) {
            try {
                real.getClass().getMethod("scanJar", Path.class, ClassLoader.class).invoke(real, jar, classLoader);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public RewriteReflectiveFacade.EnvironmentBuilder scanJar(Path jar) {
            try {
                real.getClass().getMethod("scanJar", Path.class, ClassLoader.class).invoke(real, jar, getClassLoader());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public RewriteReflectiveFacade.EnvironmentBuilder scanUserHome() {
            try {
                real.getClass().getMethod("scanUserHome").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public RewriteReflectiveFacade.EnvironmentBuilder load(RewriteReflectiveFacade.YamlResourceLoader yamlResourceLoader) {
            try {
                Class<?> resourceLoaderClass = getClassLoader().loadClass("org.openrewrite.config.ResourceLoader");
                real.getClass().getMethod("load", resourceLoaderClass).invoke(real, yamlResourceLoader.real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public RewriteReflectiveFacade.Environment build() {
            try {
                return new RewriteReflectiveFacade.Environment(real.getClass().getMethod("build").invoke(real));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class SourceFile {
        private final Object real;

        private SourceFile(Object real) {
            this.real = real;
        }

        public Path getSourcePath() {
            try {
                return (Path) real.getClass().getMethod("getSourcePath").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String print() {
            try {
                return (String) real.getClass().getMethod("print").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class Result {
        private final Object real;

        private Result(Object real) {
            this.real = real;
        }

        @Nullable
        public RewriteReflectiveFacade.SourceFile getBefore() {
            try {
                return new RewriteReflectiveFacade.SourceFile(real.getClass().getMethod("getBefore").invoke(real));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Nullable
        public RewriteReflectiveFacade.SourceFile getAfter() {
            try {
                return new RewriteReflectiveFacade.SourceFile(real.getClass().getMethod("getAfter").invoke(real));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public List<RewriteReflectiveFacade.Recipe> getRecipesThatMadeChanges() {
            try {
                Set<Object> result = (Set<Object>) real.getClass().getMethod("getRecipesThatMadeChanges").invoke(real);
                return result.stream()
                        .map(RewriteReflectiveFacade.Recipe::new)
                        .collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String diff() {
            try {
                return (String) real.getClass().getMethod("diff").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class Recipe {
        private final Object real;

        private Recipe(Object real) {
            this.real = real;
        }

        public List<RewriteReflectiveFacade.Result> run(List<RewriteReflectiveFacade.SourceFile> sources) {
            try {
                List<Object> unwrappedSources = sources.stream().map(it -> it.real).collect(toList());
                List<Object> result = (List<Object>) real.getClass().getMethod("run", List.class)
                        .invoke(real, unwrappedSources);
                return result.stream()
                        .map(RewriteReflectiveFacade.Result::new)
                        .collect(toList());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public List<RewriteReflectiveFacade.Result> run(List<RewriteReflectiveFacade.SourceFile> sources,
                                                        RewriteReflectiveFacade.InMemoryExecutionContext ctx) {
            try {
                Class<?> executionContextClass = getClassLoader().loadClass("org.openrewrite.ExecutionContext");
                List<Object> unwrappedSources = sources.stream().map(it -> it.real).collect(toList());
                List<Object> result = (List<Object>) real.getClass().getMethod("run", List.class, executionContextClass)
                        .invoke(real, unwrappedSources, ctx.real);
                return result.stream()
                        .map(RewriteReflectiveFacade.Result::new)
                        .collect(toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String getName() {
            try {
                return (String) real.getClass().getMethod("getName").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Collection<RewriteReflectiveFacade.Validated> validateAll() {
            try {
                List<Object> results = (List<Object>) real.getClass().getMethod("validateAll").invoke(real);
                return results.stream().map(r -> {
                    String canonicalName = r.getClass().getCanonicalName();
                    if (canonicalName.equals("org.openrewrite.Validated.Invalid")) {
                        return new RewriteReflectiveFacade.Validated.Invalid(r);
                    } else if (canonicalName.equals("org.openrewrite.Validated.Both")) {
                        return new RewriteReflectiveFacade.Validated.Both(r);
                    } else {
                        return null;
                    }
                }).filter(Objects::nonNull).collect(toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    public interface Validated {

        Object getReal();

        default List<RewriteReflectiveFacade.Validated.Invalid> failures() {
            try {
                Object real = getReal();
                List<Object> results = (List<Object>) real.getClass().getMethod("failures").invoke(real);
                return results.stream().map(RewriteReflectiveFacade.Validated.Invalid::new).collect(toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        class Invalid implements RewriteReflectiveFacade.Validated {

            private final Object real;

            public Invalid(Object real) {
                this.real = real;
            }

            @Override
            public Object getReal() {
                return real;
            }

            public String getProperty() {
                try {
                    return (String) real.getClass().getMethod("getProperty").invoke(real);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            public String getMessage() {
                try {
                    return (String) real.getClass().getMethod("getMessage").invoke(real);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            public Throwable getException() {
                try {
                    return (Throwable) real.getClass().getMethod("getException").invoke(real);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        class Both implements RewriteReflectiveFacade.Validated {

            private final Object real;

            public Both(Object real) {
                this.real = real;
            }

            @Override
            public Object getReal() {
                return real;
            }
        }
    }

    public class NamedStyles {
        private final Object real;

        private NamedStyles(Object real) {
            this.real = real;
        }

        public String getName() {
            try {
                return (String) real.getClass().getMethod("getName").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class Environment {
        private final Object real;

        private Environment(Object real) {
            this.real = real;
        }

        public List<RewriteReflectiveFacade.NamedStyles> activateStyles(Iterable<String> activeStyles) {
            try {
                //noinspection unchecked
                List<Object> raw = (List<Object>) real.getClass()
                        .getMethod("activateStyles", Iterable.class)
                        .invoke(real, activeStyles);
                return raw.stream()
                        .map(RewriteReflectiveFacade.NamedStyles::new)
                        .collect(toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public RewriteReflectiveFacade.Recipe activateRecipes(Iterable<String> activeRecipes) {
            try {
                return new RewriteReflectiveFacade.Recipe(real.getClass()
                        .getMethod("activateRecipes", Iterable.class)
                        .invoke(real, activeRecipes));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Collection<RewriteReflectiveFacade.RecipeDescriptor> listRecipeDescriptors() {
            try {
                Collection<Object> result = (Collection<Object>) real.getClass().getMethod("listRecipeDescriptors").invoke(real);
                return result.stream()
                        .map(RewriteReflectiveFacade.RecipeDescriptor::new)
                        .collect(toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Collection<RewriteReflectiveFacade.NamedStyles> listStyles() {
            try {
                List<Object> raw = (List<Object>) real.getClass().getMethod("listStyles").invoke(real);
                return raw.stream()
                        .map(RewriteReflectiveFacade.NamedStyles::new)
                        .collect(toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    public class RecipeDescriptor {
        private final Object real;

        private RecipeDescriptor(Object real) {
            this.real = real;
        }

        public String getName() {
            try {
                return (String) real.getClass().getMethod("getName").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String getDisplayName() {
            try {
                return (String) real.getClass().getMethod("getDisplayName").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String getDescription() {
            try {
                return (String) real.getClass().getMethod("getDescription").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public List<RewriteReflectiveFacade.OptionDescriptor> getOptions() {
            try {
                List<Object> results = (List<Object>) real.getClass().getMethod("getOptions").invoke(real);
                return results.stream().map(RewriteReflectiveFacade.OptionDescriptor::new).collect(toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class OptionDescriptor {
        private final Object real;

        private OptionDescriptor(Object real) {
            this.real = real;
        }

        public String getName() {
            try {
                return (String) real.getClass().getMethod("getName").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String getDisplayName() {
            try {
                return (String) real.getClass().getMethod("getDisplayName").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String getDescription() {
            try {
                return (String) real.getClass().getMethod("getDescription").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String getType() {
            try {
                return (String) real.getClass().getMethod("getType").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String getExample() {
            try {
                return (String) real.getClass().getMethod("getExample").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public boolean isRequired() {
            try {
                return (boolean) real.getClass().getMethod("isRequired").invoke(real);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    public RewriteReflectiveFacade.EnvironmentBuilder environmentBuilder(Properties properties) {
        try {
            return new RewriteReflectiveFacade.EnvironmentBuilder(getClassLoader()
                    .loadClass("org.openrewrite.config.Environment")
                    .getMethod("builder", Properties.class)
                    .invoke(null, properties)
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class YamlResourceLoader {
        private final Object real;

        private YamlResourceLoader(Object real) {
            this.real = real;
        }
    }

    public RewriteReflectiveFacade.YamlResourceLoader yamlResourceLoader(InputStream yamlInput, URI source, Properties properties) {
        try {
            return new RewriteReflectiveFacade.YamlResourceLoader(getClassLoader()
                    .loadClass("org.openrewrite.config.YamlResourceLoader")
                    .getConstructor(InputStream.class, URI.class, Properties.class)
                    .newInstance(yamlInput, source, properties)
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class InMemoryExecutionContext {
        private final Object real;

        private InMemoryExecutionContext(Object real) {
            this.real = real;
        }
    }

    public RewriteReflectiveFacade.InMemoryExecutionContext inMemoryExecutionContext(Consumer<Throwable> onError) {
        try {
            return new RewriteReflectiveFacade.InMemoryExecutionContext(getClassLoader()
                    .loadClass("org.openrewrite.InMemoryExecutionContext")
                    .getConstructor(Consumer.class)
                    .newInstance(onError));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class JavaParserBuilder {
        private final Object real;

        private JavaParserBuilder(Object real) {
            this.real = real;
        }

        public RewriteReflectiveFacade.JavaParserBuilder styles(List<RewriteReflectiveFacade.NamedStyles> styles) {
            try {
                List<Object> unwrappedStyles = styles.stream()
                        .map(it -> it.real)
                        .collect(toList());
                real.getClass().getMethod("styles", Iterable.class).invoke(real, unwrappedStyles);
                return this;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public RewriteReflectiveFacade.JavaParserBuilder classpath(Collection<Path> classpath) {
            try {
                real.getClass().getMethod("classpath", Collection.class).invoke(real, classpath);
                return this;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public RewriteReflectiveFacade.JavaParserBuilder logCompilationWarningsAndErrors(boolean logCompilationWarningsAndErrors) {
            try {
                real.getClass().getMethod("logCompilationWarningsAndErrors", boolean.class).invoke(real, logCompilationWarningsAndErrors);
                return this;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public RewriteReflectiveFacade.JavaParserBuilder relaxedClassTypeMatching(boolean relaxedClassTypeMatching) {
            try {
                real.getClass().getMethod("relaxedClassTypeMatching", boolean.class).invoke(real, relaxedClassTypeMatching);
                return this;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public RewriteReflectiveFacade.JavaParser build() {
            try {
                return new RewriteReflectiveFacade.JavaParser(real.getClass().getMethod("build").invoke(real));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public class JavaParser {
        private final Object real;

        private JavaParser(Object real) {
            this.real = real;
        }

        public List<RewriteReflectiveFacade.SourceFile> parse(Iterable<Path> sourcePaths, Path baseDir, RewriteReflectiveFacade.InMemoryExecutionContext ctx) {
            return parseBase(real, sourcePaths, baseDir, ctx);
        }
    }

    public RewriteReflectiveFacade.JavaParserBuilder javaParserFromJavaVersion() {
        try {
            if (System.getProperty("java.version").startsWith("1.8")) {
                return new RewriteReflectiveFacade.JavaParserBuilder(getClassLoader()
                        .loadClass("org.openrewrite.java.Java8Parser")
                        .getMethod("builder")
                        .invoke(null));
            }
            return new RewriteReflectiveFacade.JavaParserBuilder(getClassLoader()
                    .loadClass("org.openrewrite.java.Java11Parser")
                    .getMethod("builder")
                    .invoke(null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class YamlParser {
        private final Object real;

        private YamlParser(Object real) {
            this.real = real;
        }

        public List<RewriteReflectiveFacade.SourceFile> parse(Iterable<Path> sourcePaths, Path baseDir, RewriteReflectiveFacade.InMemoryExecutionContext ctx) {
            return parseBase(real, sourcePaths, baseDir, ctx);
        }
    }

    public RewriteReflectiveFacade.YamlParser yamlParser() {
        try {
            return new RewriteReflectiveFacade.YamlParser(getClassLoader().loadClass("org.openrewrite.yaml.YamlParser")
                    .getDeclaredConstructor()
                    .newInstance());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class PropertiesParser {
        private final Object real;

        private PropertiesParser(Object real) {
            this.real = real;
        }

        public List<RewriteReflectiveFacade.SourceFile> parse(Iterable<Path> sourcePaths, Path baseDir, RewriteReflectiveFacade.InMemoryExecutionContext ctx) {
            return parseBase(real, sourcePaths, baseDir, ctx);
        }
    }

    public RewriteReflectiveFacade.PropertiesParser propertiesParser() {
        try {
            return new RewriteReflectiveFacade.PropertiesParser(getClassLoader().loadClass("org.openrewrite.properties.PropertiesParser")
                    .getDeclaredConstructor()
                    .newInstance());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class XmlParser {
        private final Object real;

        private XmlParser(Object real) {
            this.real = real;
        }

        public List<RewriteReflectiveFacade.SourceFile> parse(Iterable<Path> sourcePaths, Path baseDir, RewriteReflectiveFacade.InMemoryExecutionContext ctx) {
            return parseBase(real, sourcePaths, baseDir, ctx);
        }
    }

    public RewriteReflectiveFacade.XmlParser xmlParser() {
        try {
            return new RewriteReflectiveFacade.XmlParser(getClassLoader().loadClass("org.openrewrite.xml.XmlParser")
                    .getDeclaredConstructor()
                    .newInstance());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
