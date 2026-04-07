---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-odbc-installation.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Driver installation [sql-odbc-installation]

The Elasticsearch SQL ODBC Driver can be installed on Microsoft Windows using an MSI package. The installation process is simple and is composed of standard MSI wizard steps.

## Installation Prerequisites [prerequisites]

The recommended installation platform is Windows 10 64 bit or Windows Server 2016 64 bit.

Before you install the Elasticsearch SQL ODBC Driver you need to meet the following prerequisites;

* .NET Framework 4.x full, latest - [https://dotnet.microsoft.com/download/dotnet-framework](https://dotnet.microsoft.com/download/dotnet-framework)
* Microsoft Visual C++ Redistributable for Visual Studio 2017 or later - [https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)

    * The 64 bit driver requires the x64 redistributable
    * The 32 bit driver requires the x86 or the x64 redistributable (the latter also installs the components needed for the 32 bit driver)

* Elevated privileges (administrator) for the User performing the installation.

If you fail to meet any of the prerequisites the installer will show an error message and abort the installation.

::::{note}
It is not possible to inline upgrade using the MSI. In order to upgrade, you will first have to uninstall the old driver and then install the new driver.
::::


::::{note}
When installing the MSI, the Windows Defender SmartScreen might warn about running an unrecognized app. If the MSI has been downloaded from Elastic’s web site, it is safe to acknowledge the message by allowing the installation to continue (`Run anyway`).
::::



## Version compatibility [odbc-compatibility]

Your driver must be compatible with your {{es}} version.

::::{important}
The driver version cannot be newer than the {{es}} version. For example, {{es}} version 7.10.0 is not compatible with {{version.stack}} drivers.
::::


| {{es}} version | Compatible driver versions | Example |
| --- | --- | --- |
| 7.7.0 and earlier versions | * The same version.<br> | {{es}} 7.6.1 is only compatible with 7.6.1 drivers. |


## Download the `.msi` package(s) [download]

Download the `.msi` package for Elasticsearch SQL ODBC Driver {{version.stack}} from: [https://www.elastic.co/downloads/odbc-client](   https://www.elastic.co/downloads/odbc-client)

There are two versions of the installer available:

* **32 bit driver (x86)** for use with the Microsoft Office 2016 suite of applications; notably Microsoft Excel and Microsoft Access and other 32 bit based programs.
* **64 bit driver (x64)** recommended for use with all other applications.

Users should consider downloading and installing both the 32 and 64 bit drivers for maximum compatibility across applications installed on their system.


## Installation using the graphical user interface (GUI) [installation-gui]

Double-click the downloaded `.msi` package to launch a GUI wizard that will guide you through the installation process.

You will first be presented with a welcome screen:

:::{image} ../images/elasticsearch-reference-installer_started.png
:alt: Installer Welcome Screen
:::

Clicking **Next** will present the End User License Agreement. You will need to accept the license agreement in order to continue the installation.

:::{image} ../images/elasticsearch-reference-installer_accept_license.png
:alt: Installer EULA Screen
:::

The following screen allows you to customise the installation path for the Elasticsearch ODBC driver files.

::::{note}
The default installation path is of the format: **%ProgramFiles%\Elastic\ODBCDriver\\{{version.stack}}**
::::


:::{image} ../images/elasticsearch-reference-installer_choose_destination.png
:alt: Installer Driver Path
:::

You are now ready to install the driver.

::::{note}
You will require elevated privileges (administrator) for installation.
::::


:::{image} ../images/elasticsearch-reference-installer_ready_install.png
:alt: Installer Begin
:::

Assuming the installation takes place without error you should see progress screen, followed by the finish screen:

:::{image} ../images/elasticsearch-reference-installer_installing.png
:alt: Installer Installing
:::

On the finish screen you can launch the ODBC Data Source Administration screen by checking the dialog checkbox. This will automatically launch the configuration screen on close (either 32 bit or 64 bit) where you can configure a DSN.

:::{image} ../images/elasticsearch-reference-installer_finish.png
:alt: Installer Complete
:::

As with any MSI installation package, a log file for the installation process can be found within the `%TEMP%` directory, with a randomly generated name adhering to the format `MSI<random>.LOG`.

If you encounter an error during installation we would encourage you to open an issue [https://github.com/elastic/elasticsearch-sql-odbc/issues](https://github.com/elastic/elasticsearch-sql-odbc/issues), attach your installation log file and provide additional details so we can investigate.


## Installation using the command line [installation-cmd]

::::{note}
The examples given below apply to installation of the 64 bit MSI package. To achieve the same result with the 32 bit MSI package you would instead use the filename suffix `windows-x86.msi`
::::


The `.msi` can also be installed via the command line. The simplest installation using the same defaults as the GUI is achieved by first navigating to the download directory, then running:

```sh subs=true
msiexec.exe /i esodbc-{{version.stack}}-windows-x86_64.msi /qn
```

By default, `msiexec.exe` does not wait for the installation process to complete, since it runs in the Windows subsystem. To wait on the process to finish and ensure that `%ERRORLEVEL%` is set accordingly, it is recommended to use `start /wait` to create a process and wait for it to exit:

```sh subs=true
start /wait msiexec.exe /i esodbc-{{version.stack}}-windows-x86_64.msi /qn
```

As with any MSI installation package, a log file for the installation process can be found within the `%TEMP%` directory, with a randomly generated name adhering to the format `MSI<random>.LOG`. The path to a log file can be supplied using the `/l` command line argument

```sh subs=true
start /wait msiexec.exe /i esodbc-{{version.stack}}-windows-x86_64.msi /qn /l install.log
```

Supported Windows Installer command line arguments can be viewed using:

```sh
msiexec.exe /help
```

…or by consulting the [Windows Installer SDK Command-Line Options](https://msdn.microsoft.com/en-us/library/windows/desktop/aa367988(v=vs.85).aspx).

### Command line options [odbc-msi-command-line-options]

All settings exposed within the GUI are also available as command line arguments (referred to as *properties* within Windows Installer documentation) that can be passed to `msiexec.exe`:

`INSTALLDIR`
:   The installation directory. Defaults to _%ProgramFiles%\Elastic\ODBCDriver\\{{version.stack}}_.

To pass a value, simply append the property name and value using the format `<PROPERTYNAME>="<VALUE>"` to the installation command. For example, to use a different installation directory to the default one:

```sh subs=true
start /wait msiexec.exe /i esodbc-{{version.stack}}-windows-x86_64.msi /qn INSTALLDIR="c:\CustomDirectory"
```

Consult the [Windows Installer SDK Command-Line Options](https://msdn.microsoft.com/en-us/library/windows/desktop/aa367988(v=vs.85).aspx) for additional rules related to values containing quotation marks.


### Uninstall using Add/Remove Programs [odbc-uninstall-msi-gui]

The `.msi` package handles uninstallation of all directories and files added as part of installation.

::::{warning}
Uninstallation will remove **all** contents created as part of installation.
::::


An installed program can be uninstalled by pressing the Windows key and typing `add or remove programs` to open the system settings.

Once opened, find the Elasticsearch ODBC Driver installation within the list of installed applications, click and choose `Uninstall`:

:::{image} ../images/elasticsearch-reference-uninstall.png
:alt: uninstall
:name: odbc-msi-installer-uninstall
:::


### Uninstall using the command line [odbc-uninstall-msi-command-line]

Uninstallation can also be performed from the command line by navigating to the directory containing the `.msi` package and running:

```sh subs=true
start /wait msiexec.exe /x esodbc-{{version.stack}}-windows-x86_64.msi /qn
```

Similar to the install process, a path for a log file for the uninstallation process can be passed using the `/l` command line argument

```sh subs=true
start /wait msiexec.exe /x esodbc-{{version.stack}}-windows-x86_64.msi /qn /l uninstall.log
```
