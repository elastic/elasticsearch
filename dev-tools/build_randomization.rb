#!/usr/bin/env ruby
# Licensed to  ElasticSearch and Shay Banon under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
#
# generate property file for the jdk randomization test
# 
#
require 'yaml'

class JDKSelector
  attr_reader :directory, :jdk_list

  def initialize(directory)
    @directory = directory
  end

  # get selection of available jdks from jenkins automatic install directory
  def get_jdk
    @jdk_list = Dir.entries(directory).select do |x| 
      x.chars.first == 'J' 
    end.map do |y|
      File.join(directory, y)
    end
    self
  end

  # do ranomize selection from a given array
  def select_one(selection_array = nil)
    selection_array ||= @jdk_list
    selection_array[rand(selection_array.size)]
    get_random_one(selection_array)
  end
end

def get_random_one(data_array)
  data_array[rand(data_array.size)]
end

# given a jdk directory selection, generate relevant environment variables
def get_env_matrix(data_array)
  
  #refactoring target
  es_test_jvm_option1 = get_random_one(['-server']) #only server for now get_random_one(['-client', '-server'])
  greater_than_six = File.basename([*data_array].first).split(/[^0-9]/)[-1].to_i > 6
  es_test_jvm_option2 = greater_than_six ? get_random_one(['-XX:+UseConcMarkSweepGC', '-XX:+UseParallelGC', '-XX:+UseSerialGC',
                                        '-XX:+UseG1GC']) :
                        get_random_one(['-XX:+UseConcMarkSweepGC', '-XX:+UseParallelGC', '-XX:+UseSerialGC'])

  es_test_jvm_option3 = get_random_one(['-XX:+UseCompressedOops', '-XX:-UseCompressedOops'])
  es_node_mode =  get_random_one(['local', 'network'])
  tests_nightly = get_random_one([true, false])
  tests_nightly = get_random_one([false]) #bug

  [*data_array].map do |x|
    {
      'PATH' => File.join(x,'bin') + ':' + ENV['PATH'],
      'JAVA_HOME' => x,
      'BUILD_DESC' => "%s,%s,%s%s,%s %s"%[File.basename(x), es_node_mode, tests_nightly ? 'nightly,':'',
                                            es_test_jvm_option1[1..-1], es_test_jvm_option2[4..-1], es_test_jvm_option3[4..-1]],
      'es.node.mode' => es_node_mode,
      'tests.nightly' => tests_nightly,
      'tests.jvm.argline' => "%s %s %s"%[es_test_jvm_option1, es_test_jvm_option2, es_test_jvm_option3]
    }
  end
end

# pick first element out of array of hashes, generate write java property file
def generate_property_file(directory, data)
  #array transformation
  content = data.first.map do |key, value|
    "%s=%s"%[key, value]
  end
  file_name = (ENV['BUILD_ID'] + ENV['BUILD_NUMBER']) || 'prop' rescue 'prop'
  file_name = file_name.split(File::SEPARATOR).first + '.txt'
  File.open(File.join(directory, file_name), 'w') do |file| 
              file.write(content.join("\n")) 
  end
end

# jenkins sets pwd prior to execution
jdk_selector = JDKSelector.new(File.join(ENV['PWD'],'tools','hudson.model.JDK'))
environment_matrix = get_env_matrix(jdk_selector.get_jdk.select_one)

working_directory = ENV['WORKSPACE'] || '/var/tmp'
generate_property_file(working_directory, environment_matrix)
