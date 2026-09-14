Source: ${name}
Section: ${section}
Priority: ${priority}
Maintainer: ${maintainer}
Uploaders: ${uploaders}
Version: ${fullVersion}
Standards-Version: 3.8.3
Package: ${name}<% if (provides) { %>
Provides: ${provides}<% } %>
Homepage: ${url}
Architecture: ${arch}
Distribution: ${distribution}<% if (multiArch) { %>
Multi-Arch: ${multiArch}<% } %>
Depends: ${depends}<% if (conflicts) { %>
Conflicts: ${conflicts}<% } %><% if (replaces) { %>
Replaces: ${replaces}<% } %><% if (recommends) { %>
Recommends: ${recommends}<% } %><% if (suggests) { %>
Suggests: ${suggests}<% } %><% if (enhances) { %>
Enhances: ${enhances}<% } %><% if (preDepends) { %>
Pre-Depends: ${preDepends}<% } %><% if (breaks) { %>
Breaks: ${breaks}<% } %><% customFields?.each { key, val -> %>
${key}: ${val}<% } %>
Description: ${summary}
<% description.eachLine { %>\
 ${it}
<% } %>