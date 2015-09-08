package org.elasticsearch.devtools

import org.gradle.api.DefaultTask
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.OutputFiles
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.util.PatternFilterable
import org.gradle.api.tasks.util.PatternSet

import java.util.regex.Pattern

/**
 * Checks for patterns in source files for the project which are forbidden.
 */
class ForbiddenPatternsTask extends DefaultTask {
    Map<String,String> patterns = new LinkedHashMap<>()
    PatternFilterable filesFilter = new PatternSet()

    ForbiddenPatternsTask() {
        // we always include all source files, and exclude what should not be checked
        filesFilter.include('**')
        // exclude known binary extensions
        filesFilter.exclude('**/*.gz')
        filesFilter.exclude('**/*.ico')
        filesFilter.exclude('**/*.jar')
        filesFilter.exclude('**/*.zip')

        outputs.files {
            files()
        }
    }

    /** Adds a file glob pattern to be excluded */
    void exclude(String... excludes) {
        this.filesFilter.exclude(excludes)
    }

    /** Adds pattern to forbide */
    void rule(Map<String,String> props) {
        String name = props.get('name')
        if (name == null) {
            throw new IllegalArgumentException('Missing [name] for invalid pattern rule')
        }
        String pattern = props.get('pattern')
        if (pattern == null) {
            throw new IllegalArgumentException('Missing [pattern] for invalid pattern rule')
        }
        patterns.put(name, pattern)
    }

    /** Returns the files this task will check */
    FileCollection files() {
        List<FileCollection> collections = new ArrayList<>()
        for (SourceSet sourceSet : project.sourceSets) {
            collections.add(sourceSet.allSource.matching(filesFilter))
        }
        return project.files(collections.toArray())
    }

    @TaskAction
    void checkInvalidPatterns() {
        Pattern allPatterns = Pattern.compile('(' + patterns.values().join(')|(') + ')')
        List<String> failures = new ArrayList<>()
        for (File f : files()) {
            f.eachLine { line, lineNumber ->
                if (allPatterns.matcher(line).find()) {
                    addErrorMessages(failures, f, (String)line, (int)lineNumber)
                }
            }
        }
        if (failures.isEmpty() == false) {
            throw new IllegalArgumentException('Found invalid patterns:\n' + failures.join('\n'))
        }
    }

    // iterate through patterns to find the right ones for nice error messages
    void addErrorMessages(List<String> failures, File f, String line, int lineNumber) {
        String path = project.getRootProject().projectDir.toURI().relativize(f.toURI()).toString()
        for (Map.Entry<String,String> pattern : patterns.entrySet()) {
            if (Pattern.compile(pattern.value).matcher(line).find()) {
                failures.add('- ' + pattern.key + ' on line ' + lineNumber + ' of ' + path)
            }
        }
    }
}
