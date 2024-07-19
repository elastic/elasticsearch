import java.nio.file.*
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

REPO_ROOT = "/Users/rene/dev/elastic/elasticsearch/plugins"
VERSION_PROPS = REPO_ROOT + "/../build-tools-internal/version.properties"

def parseGradleFiles(Path directory) {
    def pattern = Pattern.compile(/(\w+)\s+['"](\w[^'"]+):([^'"]+):([^'"]+)['"]/)
    def dependencies = []

    Files.walk(directory).each { path ->
        if (Files.isRegularFile(path) && path.toString().endsWith('.gradle') && path.toString().contains("plugins/examples") == false){
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

def resolveVersion(Properties props, String versionString) {
    println "Resolving version: ${versionString}"
    if(versionString.startsWith("\${versions.")) {
        def versionId = versionString.substring(versionString.indexOf('.') + 1, versionString.indexOf('}'))
        if(props.containsKey(versionId)) {
            return props.getProperty(versionId)
        } else {
            println "unknown version ${versionString} found in build.gradle file. Please add it to the version.properties file."
            return versionId
        }
    }
   
   return versionString 
}


Properties loadVersionProperties() {
    def properties = new Properties()
    def file = new File(VERSION_PROPS)
    if (!file.exists()) {
        println "The properties file '${VERSION_PROPS}' does not exist."
        return null
    }
    file.withInputStream { stream ->
        properties.load(stream)
    }
    properties.each { key, value ->
        println "Loaded version property: ${key} = ${value}"
    }
    return properties
}

def convertToCamelCase(String input) {
    def parts = input.split('-')
    def camelCaseString = parts[0]
    parts.tail().each { part ->
        // for now skip camel casing
        //camelCaseString += part.capitalize()
        camelCaseString += part
    }
    return camelCaseString
}

String calculateVersionRef(String libraryName, Map<String, String> versionCatalog, Properties properties, String version) {
    // String versionRefName = convertToCamelCase(libraryName)
    String versionRefName = libraryName
    
    if(versionCatalog.containsKey(versionRefName)) {
        def existingMajor = versionCatalog[libraryName].split("\\.")[0] as int
        def newMajor = version.split("\\.")[0] as int
        println "existingMajor: ${existingMajor}, newMajor: ${newMajor}"
        
        if(newMajor > existingMajor) {
            return versionRefName + newMajor
        } 
    } 
    return versionRefName
}

def checkOptimizations(Map<String, String> versionCatalog, Properties versionProperties) {
    def simplifications = [:]
    versionCatalog.each { givenKey, givenVersion ->
        def simpleKey = givenKey.contains("-") ? givenKey.split('-')[0] : givenKey
        def candidates = versionCatalog.findAll {k, v -> givenKey != k && k.startsWith("${simpleKey}-")}
        if(candidates.size() == 0 && versionProperties[simpleKey] != null)  {
            assert versionProperties[simpleKey] == givenVersion
            simplifications[givenKey] = simpleKey
        } else {
            candidates.each {candidateKey , candidateVersion ->
                if(candidateVersion == givenVersion) {
                    simplifications[candidateKey] = simpleKey
                }
            }
        }
        
        if(simplifications[givenKey] == null){
            def converted = convertToCamelCase(givenKey)
            
            if(givenKey != converted) {
                simplifications[givenKey] = converted
            }
        }
    }

    return simplifications
}


def parseValue(value) {
    if (value.startsWith('"') && value.endsWith('"')) {
        return value[1..-2]  // String value
    } else if (value ==~ /\d+/) {
        return value.toInteger()  // Integer value
    } else if (value ==~ /\d+\.\d+/) {
        return value.toDouble()  // Double value
    } else if (value == 'true' || value == 'false') {
        return value.toBoolean()  // Boolean value
    } else if (value.startsWith('[') && value.endsWith(']')) {
        return value[1..-2].split(',').collect { parseValue(it.trim()) }  // Array value
    } else {
        return value  // Default to string if not matched
    }
}

def parseTomlFile(filePath) {
    def tomlMap = [:]
    def currentSection = null
    def file = new File(filePath)

    file.eachLine { line ->
        line = line.trim()

        if (line.startsWith('#') || line.isEmpty()) {
            // Skip comments and empty lines
            return
        }

        if (line.startsWith('[') && line.endsWith(']')) {
            // New section
            currentSection = line[1..-2]
            tomlMap[currentSection] = [:]
        } else if (line.contains('=')) {
            // Key-value pair
            def (key, value) = line.split('=', 2).collect { it.trim() }
            value = parseValue(value)
            if (currentSection) {
                tomlMap[currentSection][key] = value
            } else {
                tomlMap[key] = value
            }
        }
    }

    return tomlMap
}

def main() {
    // def directoryPath = System.console().readLine('Enter the directory path to search for *.gradle files: ').trim()
    // def directory = Paths.get("directoryPath")
    def directory = Paths.get(REPO_ROOT)
    
    if (!Files.exists(directory) || !Files.isDirectory(directory)) {
        println "The directory '${directoryPath}' does not exist or is not a directory."
        return
    }

    def dependencies = parseGradleFiles(directory)

    def librariesCatalog = [:]
    def versionsCatalog = [:]
    
    Properties versionProperties = loadVersionProperties()
    println "Version Properties: ${versionProperties.contains('junit')}"
    if (dependencies) {
        def depsByFile = dependencies.groupBy {it.file}
        depsByFile.each { file, deps ->
            println "File: ${file}"
            deps.each { dep ->
                def effectiveVersion = resolveVersion(versionProperties, dep.version)
                def versionRefName = calculateVersionRef(dep.name, versionsCatalog, versionProperties, effectiveVersion)
                versionsCatalog.put(versionRefName, effectiveVersion)
                depLibraryEntry = [group: dep.group, name: dep.name, version:versionRefName]
                println "\"${dep.group}:${dep.name}:${dep.version}\" -> \"${depLibraryEntry}\""
                if(librariesCatalog.containsKey(versionRefName)) {
                    assert librariesCatalog[versionRefName] == depLibraryEntry
                } else {
                    librariesCatalog.put(versionRefName, depLibraryEntry)
                }
            }
            
            println ""
        }
        
        println "libraries Catalog versions" 
        
        librariesCatalog.each { key, value ->
            println "${key} = ${value}"
        }

        println "Version Catalog libraries" 
        versionsCatalog.each { key, value ->
            println "${key} = ${value}"
        }
        println "Found ${dependencies.size()} dependencies in ${depsByFile.size()} files."

    } else {
        println "No dependencies found."
    }

    def versionOptimizations = checkOptimizations(versionsCatalog, versionProperties)

    versionOptimizations.each { given, simplified ->
        println "$given -> $simplified"
        println "${versionsCatalog[simplified]}"
        if(versionsCatalog[simplified] == null) {
            versionsCatalog[simplified] = versionsCatalog[given]
        }
        versionsCatalog.remove(given)
    }

    librariesCatalog.each { key, value ->
        def simplified = versionOptimizations[key]
        if(simplified != null) {
            librariesCatalog[key].version = simplified
        }
    }

    println "\n\nversions: "
    versionsCatalog.sort().each { key, value ->
        println "${key} = \"${value}\""
    }

    librariesCatalog.sort()
    println "\n\nlibraries: "
    librariesCatalog.sort().each { k, v ->
        println "${k} = { group = \"${v['group']}\", name = \"${v['name']}\", version.ref = \"${v['version']}\" } "
    }

    // Example usage
    def tomlFilePath = '/Users/rene/dev/elastic/elasticsearch/gradle/versions.toml'
    def parsedToml = parseTomlFile(tomlFilePath)

    // Access parsed data
    existingVersions = parsedToml['versions']

// println "\n\nExisting versions:"
// existingVersions.forEach { key, value ->
//     println "${key} = ${value}"
// }

// existingLibs = parsedToml['libraries']

// existingLibs.forEach { key, value ->
//     println "${key} = ${value}"
// }

def finalVersions = [:]
def finalLibraries = [:]

existingVersions.each { key, value ->
    finalVersions[key] = value
    if(versionsCatalog.containsKey(key)) {
        assert value == versionsCatalog[key]
        versionsCatalog.remove(key)
    }
}
finalVersions.putAll(versionsCatalog)


println "\n\n[versions]"
finalVersions.sort().each { key, value ->
    println "${key} = \"${value}\""
}

def existingLibs = parsedToml['libraries']
existingLibs.each { key, value ->
    finalLibraries[key] = value
    if(librariesCatalog[key] != null) {
        def newValue = librariesCatalog[key]
        assert value == "{ group = \"${newValue['group']}\", name = \"${newValue['name']}\", version.ref = \"${newValue['version']}\" }"
        librariesCatalog.remove(key)
    }
}
finalLibraries.putAll(librariesCatalog)

println "\n\n[libraries]"
finalLibraries.sort().each { key, value ->
    if(value instanceof Map) {
        println "${key} = { group = \"${value['group']}\", name = \"${value['name']}\", version.ref = \"${value['version']}\" }"
    } else if (value.startsWith("{")) {
        println "${key} = $value"
    } else {
        println "${key} = \"$value\""
    }
}

// println "Title: ${parsedToml['title']}"
// println "Owner Name: ${parsedToml['versions']['name']}"
// println "Database Server: ${parsedToml['database']['server']}"
// println "Database Ports: ${parsedToml['database']['ports']}"

}

main()