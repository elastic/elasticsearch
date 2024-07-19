import groovy.io.FileType
import java.nio.file.*
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

// Define the base directory to start the search
def baseDir = new File('/Users/rene/dev/elastic/elasticsearch/plugins')
    def pattern = Pattern.compile(/(\w+)\s+['"](\w[^'"]+):([^'"]+):([^'"]+)['"]/)

// Define the pattern to match dependencies
def dependencyPattern = ~/(\w+)\s+['"](\w[^'"]+):([^'"]+):([^'"]+)['"]/

baseDir.eachFileRecurse(FileType.FILES) { file ->
    if (file.name.endsWith('.gradle')) {
        def content = file.text
        def newContent = content.replaceAll(dependencyPattern) { match, config, group, name, version ->
            def libName = "${name.replaceAll('-', '.')}".toLowerCase()
            "$config libs.${libName}"
        }
        file.text = newContent
    }
}

println "Dependency patterns replaced successfully."