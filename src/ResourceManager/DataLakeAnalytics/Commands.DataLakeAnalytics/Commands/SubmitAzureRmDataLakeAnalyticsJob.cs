// ----------------------------------------------------------------------------------
//
// Copyright Microsoft Corporation
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ----------------------------------------------------------------------------------

using Microsoft.Azure.Commands.DataLakeAnalytics.Models;
using Microsoft.Azure.Commands.DataLakeAnalytics.Properties;
using Microsoft.Azure.Commands.ResourceManager.Common.Tags;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Rest.Azure;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Management.Automation;

namespace Microsoft.Azure.Commands.DataLakeAnalytics
{
    [Cmdlet(VerbsLifecycle.Submit, "AzureRmDataLakeAnalyticsJob"), OutputType(typeof(JobInformation))]
    [Alias("Submit-AdlJob")]
    public class SubmitAzureDataLakeAnalyticsJob : DataLakeAnalyticsCmdletBase
    {
        internal const string JobWithScriptPath = "Submit job with script path";
        internal const string JobWithInlineScriptParameterSetName = "Submit Job with in-line script";
        private int _degreeOfParallelism = 1;
        private int _priority = 1000;

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 0,
            Mandatory = true, HelpMessage = "Name of Data Lake Analytics account under which the job will be submitted."
            )]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Position = 0,
            Mandatory = true, HelpMessage = "Name of Data Lake Analytics account under which the job will be submitted."
            )]
        [ValidateNotNullOrEmpty]
        [Alias("AccountName")]
        public string Account { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 1,
            Mandatory = true, HelpMessage = "The friendly name of the job to submit.")]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Position = 1,
            Mandatory = true, HelpMessage = "The friendly name of the job to submit.")]
        [ValidateNotNullOrEmpty]
        public string Name { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 2,
            Mandatory = true, HelpMessage = "Path to the script file to submit.")]
        [ValidateNotNullOrEmpty]
        public string ScriptPath { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, ValueFromPipeline = true, Position = 2,
            ParameterSetName = JobWithInlineScriptParameterSetName, Mandatory = true,
            HelpMessage = "Script to execute (written inline).")]
        [ValidateNotNullOrEmpty]
        public string Script { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Mandatory = true, HelpMessage = "Specifies the type of job being submitted (such as USql or Hive).")]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Mandatory = true, HelpMessage = "Specifies the type of job being submitted (such as USql or Hive).")]
        public JobType Type { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 3,
            Mandatory = false,
            HelpMessage =
                "Optionally set the version of the runtime to use for the job. If left unset, the default runtime is used."
            )]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Position = 3,
            Mandatory = false,
            HelpMessage =
                "Optionally set the version of the runtime to use for the job. If left unset, the default runtime is used."
            )]
        [ValidateNotNullOrEmpty]
        public string Runtime { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 4,
            Mandatory = false,
            HelpMessage =
                "The type of compilation to be done on this job. Valid values are: 'Semantic' (Only erforms semantic checks and necessary sanity checks), 'Full' (full compilation) and 'SingleBox' (Full compilation performed locally)."
            )]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Position = 4,
            Mandatory = false,
            HelpMessage =
                "The type of compilation to be done on this job. Valid values are: 'Semantic' (Only erforms semantic checks and necessary sanity checks), 'Full' (full compilation) and 'SingleBox' (Full compilation performed locally)"
            )]
        [ValidateSet("Semantic", "SingleBox", "Full")]
        public string CompileMode { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 5,
            Mandatory = false,
            HelpMessage = "Indicates that the submission should only build the job and not execute if set to true.")]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Position = 5,
            Mandatory = false,
            HelpMessage = "Indicates that the submission should only build the job and not execute if set to true.")]
        [ValidateNotNullOrEmpty]
        public SwitchParameter CompileOnly { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 6,
            Mandatory = false,
            HelpMessage =
                "The degree of parallelism to use for this job. Typically, a higher degree of parallelism dedicated to a script results in faster script execution time."
            )]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Position = 6,
            Mandatory = false,
            HelpMessage =
                "The degree of parallelism to use for this job. Typically, a higher degree of parallelism dedicated to a script results in faster script execution time."
            )]
        public int DegreeOfParallelism
        {
            get { return _degreeOfParallelism; }
            set { _degreeOfParallelism = value; }
        }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 7,
            Mandatory = false,
            HelpMessage =
                "The priority for this job with a range from 1 to 1000, where 1000 is the lowest priority and 1 is the highest."
            )]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Position = 7,
            Mandatory = false,
            HelpMessage =
                "The priority for this job with a range from 1 to 1000, where 1000 is the lowest priority and 1 is the highest."
            )]
        [ValidateRange(1, int.MaxValue)]
        public int Priority
        {
            get { return _priority; }
            set { _priority = value; }
        }

        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithScriptPath, Position = 8,
            Mandatory = false,
            HelpMessage = "The custom configurations to use for this job in key/value pairs. This is currently only supported for Hive jobs")]
        [Parameter(ValueFromPipelineByPropertyName = true, ParameterSetName = JobWithInlineScriptParameterSetName, Position = 8,
            Mandatory = false,
            HelpMessage = "The custom configurations to use for this job in key / value pairs.This is currently only supported for Hive jobs")]
        [ValidateNotNullOrEmpty]
        public Hashtable Configurations { get; set; }

        public override void ExecuteCmdlet()
        {
            // error handling for not passing or passing both script and script path
            if ((string.IsNullOrEmpty(Script) && string.IsNullOrEmpty(ScriptPath)) ||
                (!string.IsNullOrEmpty(Script) && !string.IsNullOrEmpty(ScriptPath)))
            {
                throw new CloudException(Resources.AmbiguousScriptParameter);
            }

            // get the script
            if (string.IsNullOrEmpty(Script))
            {
                var powerShellDestinationPath = SessionState.Path.GetUnresolvedProviderPathFromPSPath(ScriptPath);
                if (!File.Exists(powerShellDestinationPath))
                {
                    throw new CloudException(string.Format(Resources.ScriptFilePathDoesNotExist,
                        powerShellDestinationPath));
                }

                Script = File.ReadAllText(powerShellDestinationPath);
            }

            JobProperties properties;
            switch(this.Type)
            { 
                case JobType.USql:
                    if(Configurations != null && Configurations.Count > 0)
                    {
                        WriteWarningWithTimestamp(Resources.JobConfigurationPropertyWarning);
                    }

                    var sqlIpProperties = new USqlJobProperties
                    {
                        Script = Script
                    };

                    if (!string.IsNullOrEmpty(CompileMode))
                    {
                        CompileMode toUse;
                        if (Enum.TryParse(CompileMode, out toUse))
                        {
                            sqlIpProperties.CompileMode = toUse;
                        }
                    }

                    if (!string.IsNullOrEmpty(Runtime))
                    {
                        sqlIpProperties.RuntimeVersion = Runtime;
                    }

                    properties = sqlIpProperties;
                    break;
                case JobType.Hive:
                    var convertedConfig = TagsConversionHelper.CreateTagDictionary(Configurations, true);
                    if (convertedConfig == null)
                    {
                        convertedConfig = new Dictionary<string, string>();
                    }
                    properties = new HiveJobProperties
                    {
                        Script = Script,
                        Configurations = convertedConfig
                    };

                    if (!string.IsNullOrEmpty(Runtime))
                    {
                        properties.RuntimeVersion = Runtime;
                    }

                    break;
                default:
                    throw new CloudException(string.Format(Resources.InvalidJobType, this.Type));
            }
            var jobInfo = new JobInformation
            (
                jobId: DataLakeAnalyticsClient.JobIdQueue.Count == 0 ? Guid.NewGuid() : DataLakeAnalyticsClient.JobIdQueue.Dequeue(),
                name: Name,
                properties: properties,
                type: this.Type,
                degreeOfParallelism: DegreeOfParallelism,
                priority: Priority
            );

            // TODO: Confirm that hive jobs support "build" functionality.
            if (CompileOnly && this.Type == JobType.Hive)
            {
                throw new InvalidOperationException(Resources.CannotCompileHive);
            }
            
            WriteObject(CompileOnly
                ? DataLakeAnalyticsClient.BuildJob(Account, jobInfo)
                : DataLakeAnalyticsClient.SubmitJob(Account, jobInfo));
        }
    }
}