from mrjob.job import MRJob
from mrjob.step import MRStep  # we need this as we are doing multistep job
import re

word_regexp = re.compile(r"[\w']+")

class word_count(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words,
                   reducer = self.reducer_count_words),
            MRStep(mapper = self.mapper_make_count_key,
                   reducer = self.reducer_output_words),
        ]
    
    def mapper_get_words(self, _, line):
        words = word_regexp.findall(line)
        for word in words:
            yield word.lower(), 1
    
    def reducer_count_words(self, word, value):
        yield word, sum(value)
    
    def mapper_make_count_key(self, word, value):
        yield '%04d'%int(value), word
    
    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word


if __name__ == '__main__':
    word_count.run()

