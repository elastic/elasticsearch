#!/bin/bash

cd /opt/shibboleth-idp/bin

echo "Please complete the following for your IdP environment:"
./ant.sh -Didp.target.dir=/opt/shibboleth-idp-tmp -Didp.src.dir=/opt/shibboleth-idp/ install

find /opt/shibboleth-idp-tmp/ -type d -exec chmod 750 {} \;

mkdir -p /ext-mount/customized-shibboleth-idp/conf/
chmod -R 750 /ext-mount/customized-shibboleth-idp/

# Copy the essential and routinely customized config to out Docker mount.
cd /opt/shibboleth-idp-tmp
cp -r credentials/ /ext-mount/customized-shibboleth-idp/
cp -r metadata/ /ext-mount/customized-shibboleth-idp/
cp conf/{attribute-resolver*.xml,attribute-filter.xml,cas-protocol.xml,idp.properties,ldap.properties,metadata-providers.xml,relying-party.xml,saml-nameid.*} /ext-mount/customized-shibboleth-idp/conf/

# Copy the basic UI components, which are routinely customized
cp -r views/ /ext-mount/customized-shibboleth-idp/
mkdir /ext-mount/customized-shibboleth-idp/webapp/
cp -r edit-webapp/css/ /ext-mount/customized-shibboleth-idp/webapp/
cp -r edit-webapp/images/ /ext-mount/customized-shibboleth-idp/webapp/
rm -r /ext-mount/customized-shibboleth-idp/views/user-prefs.js

echo "A basic Shibboleth IdP config and UI has been copied to ./customized-shibboleth-idp/ (assuming the default volume mapping was used)."
echo "Most files, if not being customized can be removed from what was exported/the local Docker image and baseline files will be used."
