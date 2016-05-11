require "bundler/gem_tasks"
require "rspec/core/rake_task"
require 'rake/extensiontask'

RSpec::Core::RakeTask.new(spec: :compile)

task :default => :spec

Rake::ExtensionTask.new 'ckafka' do |ext|
  ext.lib_dir = "lib/ckafka"
end
