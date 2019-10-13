#r "paket:
        nuget Fake.Core.Target              5.17.0 
        nuget Fake.Core.ReleaseNotes 
        nuget Fake.DotNet.Cli
        nuget Fake.DotNet.AssemblyInfoFile //"

#load "./.fake/build.fsx/intellisense.fsx"

open Fake.Core
open Fake.Core.TargetOperators

open Fake.DotNet

let ProjectFile = "KG.System.Data.SqlClient.Extensions.ReaderWrapper.csproj"

let BuildTarget = Target.create
let log         = Trace.log

let getVersion = 
    log "Getting Version"

    ReleaseNotes.load("ReleaseNotes.md")

BuildTarget "Clean" (fun _ ->
    log "Clean Dirs"
)

BuildTarget "AssemblyInfo" (fun _ ->
    log "updating assembly info"

    AssemblyInfoFile.createCSharp "./Properties/AssmeblyInfo.cs"
        [
            AssemblyInfo.Title          "System.Data.SqlClient.Extensions.ReaderWrapper"
            AssemblyInfo.Description    "ReaderWrapper Extension for System.Data.SqlClient"
            AssemblyInfo.Guid           "0E1ECA29-37B2-4AA8-821C-9835856B98C7"
            AssemblyInfo.Product        "ReaderWrapper"
            AssemblyInfo.Version        getVersion.AssemblyVersion
            AssemblyInfo.FileVersion    getVersion.AssemblyVersion
        ]
)

BuildTarget "DotnetPack" (fun _ ->
    log "run dotnet pack command"

    let version = getVersion.NugetVersion

    let msBuildParams (defaults:MSBuild.CliArguments) = 
        { defaults with 
            Properties = [
                "PackageVersion", version
            ]
        }

    let packOptions (defaults:DotNet.PackOptions) =
        {defaults with
            OutputPath = Some "./deploy"
            MSBuildParams = msBuildParams defaults.MSBuildParams
        }

    DotNet.pack packOptions ProjectFile
)

BuildTarget "NugetDeploy" (fun _ ->
    log "run nuget deploy"
)

"Clean"
==> "AssemblyInfo"
==> "DotnetPack"
==> "NugetDeploy"

// start build
Target.runOrDefaultWithArguments "DotnetPack"