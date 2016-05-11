# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'ckafka/version'

Gem::Specification.new do |spec|
  spec.name          = "ckafka"
  spec.version       = Ckafka::VERSION
  spec.authors       = ["Ian Quick"]
  spec.email         = ["ian.quick@gmail.com"]

  spec.summary       = "Simple Kafka Producer"
  spec.description   = "interface to the rdkafka C library"
  spec.homepage      = "https://github.com/ibawt/ckafka"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = %w(ext/ckafka/extconf.rb)
  spec.add_development_dependency "bundler", "~> 1.9"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency 'rake-compiler'
  spec.add_development_dependency 'minitest', '~> 5.8'
  spec.add_development_dependency "poseidon", "= 0.0.5"
end
