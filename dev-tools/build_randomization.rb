#!/usr/bin/env ruby
# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance  with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on
# an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License
#
# NAME
#    build_randomization.rb --  Generate property file for the JDK randomization test
#
# SYNOPSIS
#    build_randomization.rb [-d] [-l|t]
#
# DESCRIPTION
#    This script takes the randomization choices described in RANDOM_CHOICE and generates apporpriate JAVA property file 'prop.txt'
#    This property file also contain the appropriate JDK selection, randomized.  JDK randomization is based on what is available on the Jenkins tools
#    directory.  This script is used by Jenkins test system to conduct Elasticsearch server randomization testing.
#
#    In hash RANDOM_CHOISES, the key of randomization hash maps to key of java property.  The value of the hash describes the possible value of the randomization
#
#    For example  RANDOM_CHOICES = { 'es.node.mode' => {:choices => ['local', 'network'], :method => :get_random_one} } means
#    es.node.mode will be set to either 'local' or 'network', each with 50% of probability
#
# OPTIONS SUMMARY
#    The options are as follows:
#
#       -d, --debug   Increase logging verbosity for debugging purpose
#       -t, --test    Run in test mode.  The script will execute unit tests.
#       -l, --local   Run in local mode.  In this mode, directory structure will be created under current directory to mimick 
#                     Jenkins' server directory layout. This mode is mainly used for development.
require 'enumerator'
require 'getoptlong'
require 'log4r'
require 'optparse'
require 'rubygems'
require 'yaml'
include Log4r

RANDOM_CHOICES = {
  'tests.jvm.argline' => [
                {:choices => ['-server'], :method => 'get_random_one'},
                {:choices => ['-XX:+UseConcMarkSweepGC', '-XX:+UseParallelGC', '-XX:+UseSerialGC', '-XX:+UseG1GC'], :method => 'get_random_one'},
                {:choices => ['-XX:+UseCompressedOops', '-XX:-UseCompressedOops'], :method => 'get_random_one'},
                {:choices => ['-XX:+AggressiveOpts'], :method => 'get_50_percent'}
               ],

  'es.node.mode' => {:choices => ['local', 'network'], :method => 'get_random_one'},

  # bug forced to be false for now :test_nightly => { :method => :true_or_false},
  'tests.nightly' => {:selections => false},
  'tests.heap.size' => {:choices => [512, 1024], :method => :random_heap},
  'tests.assertion.disabled'=> {:choices => 'org.elasticsearch', :method => 'get_10_percent'},
  'tests.security.manager' => {:choices => [true, false], :method => 'get_90_percent'},
}

L = Logger.new 'test_randomizer'
L.outputters = Outputter.stdout
L.level = INFO
C = {:local => false, :test => false}


OptionParser.new do |opts|
  opts.banner = "Usage: build_ranodimzatin.rb [options]"

  opts.on("-d", "--debug", "Debug mode") do |d|
    L.level = DEBUG
  end

  opts.on("-l", "--local", "Run in local mode") do |l|
    C[:local] = true
  end

  opts.on("-t", "--test", "Run unit tests") do |t|
    C[:test] = true
  end
end.parse!

class Randomizer
  attr_accessor :data_array

  def initialize(data_array)
    @data_array = data_array
  end

  def true_or_false
    [true, false][rand(2)]
  end

  def random_heap
    inner_data_array = [data_array[0], data_array[1], data_array[0] + rand(data_array[1] - data_array[0])]
    "%sm" % inner_data_array[rand(inner_data_array.size)]
  end

  def get_random_with_distribution(mdata_array, distribution)
    L.debug "randomized distribution data %s" % YAML.dump(mdata_array)
    L.debug "randomized distribution distribution %s" % YAML.dump(distribution)
    carry = 0
    distribution_map = distribution.enum_for(:each_with_index).map { |x,i|  pre_carry = carry ; carry += x; {i => x + pre_carry} }

    random_size = distribution_map.last.values.first
    selection = rand(random_size)
    #get the index that randomize choice mapped to
    choice = distribution_map.select do |x|
      x.values.first > selection   #only keep the index with distribution value that is higher than the random generated number
    end.first.keys.first #first hash's first key is the index we want

    L.debug("randomized distribution choice %s" % mdata_array[choice])
    mdata_array[choice]
  end

  def get_random_one
    data_array[rand(data_array.size)]
  end

  def method_missing(meth, *args, &block)
    # trap randomization based on percentage
    if meth.to_s =~ /^get_(\d+)_percent/
      percentage = $1.to_i
      remain = 100 - percentage
      #data = args.first
      normalized_data = if(!data_array.kind_of?(Array))
                   [data_array, nil]
                 else
                   data_array
                 end
      get_random_with_distribution(normalized_data, [percentage, remain])
    else
      super
    end
  end

end

class JDKSelector
  attr_reader :directory, :jdk_list

  def initialize(directory)
    @directory = directory
  end

  # get selection of available JDKs from Jenkins automatic install directory
  def get_jdk
    @jdk_list = Dir.entries(directory).select do |x|
      x.chars.first == 'J'
    end.map do |y|
      File.join(directory, y)
    end
    self
  end

  def filter_java_6(files)
    files.select{ |i| File.basename(i).split(/[^0-9]/)[-1].to_i > 6 }
  end

  # do randomized selection from a given array
  def select_one(selection_array = nil)
    selection_array = filter_java_6(selection_array || @jdk_list)
    Randomizer.new(selection_array).get_random_one
  end

  def JDKSelector.generate_jdk_hash(jdk_choice)
    file_separator = if Gem.win_platform?
                       File::ALT_SEPARATOR
                     else
                       File::SEPARATOR
                     end
    {
      :PATH => [jdk_choice, 'bin'].join(file_separator) + File::PATH_SEPARATOR + ENV['PATH'],
      :JAVA_HOME => jdk_choice
    }
  end
end

#
# Fix argument JDK selector
#
class FixedJDKSelector < JDKSelector
  def initialize(directory)
    @directory = [*directory] #selection of directories to pick from
  end

  def get_jdk
    #since JDK selection is already specified..jdk list is the @directory
    @jdk_list = @directory
    self
  end

  def select_one(selection_array = nil)
    #bypass filtering since this is not automatic
    selection_array ||= @jdk_list
    Randomizer.new(selection_array).get_random_one
  end
end

#
# Property file writer
#
class PropertyWriter
  attr_reader :working_directory

  def initialize(mworking_directory)
    @working_directory = mworking_directory
  end

  # # pick first element out of array of hashes, generate write java property file
  def generate_property_file(data)
    directory = working_directory

    #array transformation
    content = data.to_a.map do |x|
      x.join('=')
    end.sort
    file_name = (ENV['BUILD_ID'] + ENV['BUILD_NUMBER']) || 'prop' rescue 'prop'
    file_name = file_name.split(File::SEPARATOR).first + '.txt'
    L.debug "Property file name is %s" % file_name
    File.open(File.join(directory, file_name), 'w') do |file|
      file.write(content.join("\n"))
    end
  end
end

#
# Execute randomization logics
#
class RandomizedRunner
  attr_reader :random_choices, :jdk, :p_writer

  def initialize(mrandom_choices, mjdk, mwriter)
    @random_choices = mrandom_choices
    @jdk = mjdk
    @p_writer = mwriter
  end

  def generate_selections
    configuration = random_choices

    L.debug "Enter %s" % __method__
    L.debug "Configuration %s" % YAML.dump(configuration)

    generated = {}
    configuration.each do |k, v|
      if(v.kind_of?(Hash))
        if(v.has_key?(:method))
          randomizer = Randomizer.new(v[:choices])
          v[:selections] = randomizer.__send__(v[:method])
        end
      else
        v.each do |x|
          if(x.has_key?(:method))
            randomizer = Randomizer.new(x[:choices])
            x[:selections] = randomizer.__send__(x[:method])
          end
        end
      end
    end.each do |k, v|
      if(v.kind_of?(Array))
        selections = v.inject([]) do |sum, current_hash|
          sum.push(current_hash[:selections])
        end
      else
        selections = [v[:selections]] unless v[:selections].nil?
      end
      generated[k] = selections unless (selections.nil? || selections.size == 0)
    end

    L.debug "Generated selections %s" % YAML.dump(generated)
    generated
  end

  def get_env_matrix(jdk_selection, selections)
    L.debug "Enter %s" % __method__

    #normalization
    s = {}
    selections.each do |k, v|
      if(v.size > 1)
        s[k] = v.compact.join(' ') #this should be dependent on class of v[0] and perform reduce operation instead... good enough for now
      else
        s[k] = v.first
      end
    end
    j = JDKSelector.generate_jdk_hash(jdk_selection)

    # create build description line
    desc = {}

    # TODO: better error handling
    desc[:BUILD_DESC] = "%s,%s,heap[%s],%s%s%s%s" % [
                                            File.basename(j[:JAVA_HOME]),
                                            s['es.node.mode'],
                                            s['tests.heap.size'],
                                            s['tests.nightly'] ? 'nightly,':'',
                                            s['tests.jvm.argline'].gsub(/-XX:/,''),
                                            s.has_key?('tests.assertion.disabled')? ',assert off' : '',
                                            s['tests.security.manager'] ? ',sec manager on' : ''
                                           ]
    result = j.merge(s).merge(desc)
    L.debug(YAML.dump(result))
    result
  end

  def run!
    p_writer.generate_property_file(get_env_matrix(jdk, generate_selections))
  end

end


#
# Main
#
unless(C[:test])

  # Check to see if this is running locally
  unless(C[:local])
    L.debug("Normal Mode")
    working_directory = ENV.fetch('WORKSPACE', (Gem.win_platform? ? Dir.pwd : '/var/tmp'))
  else
    L.debug("Local Mode")
    test_directory = 'tools/hudson.model.JDK/'
    unless(File.exist?(test_directory))
      L.info "running local mode, setting up running environment"
      L.info "properties are written to file prop.txt"
      FileUtils.mkpath "%sJDK6" % test_directory
      FileUtils.mkpath "%sJDK7" % test_directory
    end
    working_directory = Dir.pwd
  end


  # script support both window and linux
  # TODO: refactor into platform/machine dependent class structure
  jdk = if(Gem.win_platform?)
          #window mode jdk directories are fixed
          #TODO: better logic
          L.debug("Window Mode")
          if(File.directory?('y:\jdk7\7u55'))   #old window system under ec2
             FixedJDKSelector.new('y:\jdk7\7u55')
          else  #new metal window system
             FixedJDKSelector.new(['c:\PROGRA~1\JAVA\jdk1.8.0_05', 'c:\PROGRA~1\JAVA\jdk1.7.0_55'])
          end
        else
          #Jenkins sets pwd prior to execution
          L.debug("Linux Mode")
          JDKSelector.new(File.join(ENV['PWD'],'tools','hudson.model.JDK'))
        end

  runner = RandomizedRunner.new(RANDOM_CHOICES,
                               jdk.get_jdk.select_one,
                               PropertyWriter.new(working_directory))
  environment_matrix = runner.run!
  exit 0
else
  require "test/unit"
end

#
# Test
#
class TestJDKSelector < Test::Unit::TestCase
  L = Logger.new 'test'
  L.outputters = Outputter.stdout
  L.level = DEBUG

  def test_hash_generator
    jdk_choice = '/dummy/jdk7'
    generated = JDKSelector.generate_jdk_hash(jdk_choice)
    L.debug "Generated %s" % generated
    assert generated[:PATH].include?(jdk_choice), "PATH doesn't included choice"
    assert generated[:JAVA_HOME].include?(jdk_choice), "JAVA home doesn't include choice"
  end
end

class TestFixJDKSelector < Test::Unit::TestCase
  L = Logger.new 'test'
  L.outputters = Outputter.stdout
  L.level = DEBUG

  def test_initialize
    ['/home/dummy', ['/JDK7', '/home2'], ['home/dummy']].each do |x|
      test_object = FixedJDKSelector.new(x)
      assert_kind_of Array, test_object.directory
      assert_equal [*x], test_object.directory
    end
  end

  def test_select_one
    test_array = %w(one two three)
    test_object = FixedJDKSelector.new(test_array)
    assert test_array.include?(test_object.get_jdk.select_one)
  end

  def test_hash_generator
    jdk_choice = '/dummy/jdk7'
    generated = FixedJDKSelector.generate_jdk_hash(jdk_choice)
    L.debug "Generated %s" % generated
    assert generated[:PATH].include?(jdk_choice), "PATH doesn't included choice"
    assert generated[:JAVA_HOME].include?(jdk_choice), "JAVA home doesn't include choice"
  end
end

class TestPropertyWriter < Test::Unit::TestCase
  L = Logger.new 'test'
  L.outputters = Outputter.stdout
  L.level = DEBUG

  def test_initialize
    ['/home/dummy','/tmp'].each do |x|
      test_object = PropertyWriter.new(x)
      assert_kind_of String, test_object.working_directory
      assert_equal x, test_object.working_directory
    end
  end

  def test_generate_property
    test_file = '/tmp/prop.txt'
    File.delete(test_file) if File.exist?(test_file)
    test_object = PropertyWriter.new(File.dirname(test_file))
    # default prop.txt
    test_object.generate_property_file({:hi => 'there'})
    assert(File.exist?(test_file))

    File.open(test_file, 'r') do |properties_file|
      properties_file.read.each_line do |line|
        line.strip!
        assert_equal 'hi=there', line, "content %s is not hi=there" % line
      end
    end
    File.delete(test_file) if File.exist?(test_file)
  end
end

class DummyPropertyWriter < PropertyWriter
  def generate_property_file(data)
    L.debug "generating property file for %s" % YAML.dump(data) 
    L.debug "on directory %s" % working_directory
  end
end

class TestRandomizedRunner < Test::Unit::TestCase

  def test_initialize
    test_object = RandomizedRunner.new(RANDOM_CHOICES, '/tmp/dummy/jdk', po = PropertyWriter.new('/tmp'))
    assert_equal RANDOM_CHOICES, test_object.random_choices
    assert_equal '/tmp/dummy/jdk', test_object.jdk
    assert_equal po, test_object.p_writer
  end

  def test_generate_selection_no_method
    test_object = RandomizedRunner.new({'tests.one' => {:selections => false }}, '/tmp/dummy/jdk', po = DummyPropertyWriter.new('/tmp'))
    selection =  test_object.generate_selections
    assert_equal false, selection['tests.one'].first, 'randomization without selection method fails'
  end

  def test_generate_with_method
    test_object = RandomizedRunner.new({'es.node.mode' => {:choices => ['local', 'network'], :method => 'get_random_one'}}, 
                                      '/tmp/dummy/jdk', po = DummyPropertyWriter.new('/tmp'))
    selection =  test_object.generate_selections
    assert ['local', 'network'].include?(selection['es.node.mode'].first), 'selection choice is not correct'
  end

  def test_get_env_matrix
    test_object = RandomizedRunner.new(RANDOM_CHOICES,
                                      '/tmp/dummy/jdk', po = DummyPropertyWriter.new('/tmp'))
    selection =  test_object.generate_selections
    env_matrix = test_object.get_env_matrix('/tmp/dummy/jdk', selection)
    puts YAML.dump(env_matrix)
    assert_equal '/tmp/dummy/jdk', env_matrix[:JAVA_HOME]
  end

end
