
var gulp = require('gulp');
var shell = require('gulp-shell')

var exec = require('child_process').exec;












gulp.task('one', shell.task([


  'sudo $SPARK_HOME/bin/spark-submit invoices.py',



]));

gulp.task('sync', function (cb) {
  // setTimeout could be any async task
  setTimeout(function () {
    cb();
  }, 50000);
});

gulp.task('two',['sync'], shell.task([


  ' gulp run --cwd ElectronAngular/',


]));

gulp.task('default', [ 'one','sync', 'two' ]);




