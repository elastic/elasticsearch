import java.nio.file.*
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

def parseGradleFiles(Path directory) {
    List<String> configurations = ['api', 
    'implementation',
    "testImplementation",
    "testRuntimeOnly",
    "runtimeOnly"]

    def configsRexex = configurations.join('|')
    def pattern = Pattern.compile(/(\w+)\s+['"](\w[^'"]+):([^'"]+):([^'"]+)['"]/)
    def dependencies = []

    Files.walk(directory).each { path ->
        if (Files.isRegularFile(path) && path.toString().endsWith('.gradle')) {
            def lines = Files.readAllLines(path, StandardCharsets.UTF_8)
            lines.each { line ->
                def matcher = pattern.matcher(line)
                if (matcher.find()) {
                    def configuration = matcher.group(1)
                    def group = matcher.group(2)
                    def name = matcher.group(3)
                    def version = matcher.group(4)
                    dependencies << [file: path.toString(), configuration: configuration, group: group, name: name, version: version]
                }
            }
        }
    }
    return dependencies
}

String convertToVersionCatalogEntry(def dependencies) {
    Set<String> versions = new TreeSet<>()
    Set<String> entries = new TreeSet<>()

}

def main() {
    // def directoryPath = System.console().readLine('Enter the directory path to search for *.gradle files: ').trim()
    // def directory = Paths.get("directoryPath")
    def directory = Paths.get("/Users/rene/dev/elastic/elasticsearch")
    
    if (!Files.exists(directory) || !Files.isDirectory(directory)) {
        println "The directory '${directoryPath}' does not exist or is not a directory."
        return
    }

    def dependencies = parseGradleFiles(directory)
    if (dependencies) {
        def depsByFile = dependencies.groupBy {it.file}
        depsByFile.each { file, deps ->
            println "File: ${file}"
            deps.each { dep ->
                println "${dep.configuration} '${dep.group}:${dep.name}:${dep.version}'"
            }
            println ""
        }
        
        println "Found ${dependencies.size()} dependencies in ${depsByFile.size()} files."

    } else {
        println "No dependencies found."
    }
}

main()