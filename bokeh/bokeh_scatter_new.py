#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
 * Created by kevin on 11/11/16.
"""
from bokeh.charts import output_file, show, marker
from bokeh.sampledata.autompg import autompg as df

from bokeh.charts.glyphs import PointGlyph
from bokeh.charts.attributes import MarkerAttr, ColorAttr
from bokeh.charts.builder import create_and_build, XYBuilder
from bokeh.charts.utils import add_tooltips_columns

# 修改此处的size值可以改变scatter中marker的size的大小
MARK_SIZE = 20


class MyScatterBuilder(XYBuilder):
    default_attributes = {'color': ColorAttr(),
                          'marker': MarkerAttr()}

    def yield_renderers(self):
        for group in self._data.groupby(**self.attributes):

            glyph = PointGlyph(label=group.label,
                               x=group.get_values(self.x.selection),
                               y=group.get_values(self.y.selection),
                               line_color=group['color'],
                               fill_color=group['color'],
                               marker=group['marker'],
                               size=MARK_SIZE)

            self.add_glyph(group, glyph)

            for renderer in glyph.renderers:

                if self.tooltips:
                    renderer = add_tooltips_columns(renderer, self.tooltips, group)

                yield renderer


def MyScatter(data=None, x=None, y=None, **kws):
    kws['x'] = x
    kws['y'] = y
    return create_and_build(MyScatterBuilder, data, **kws)


if __name__ == '__main__':
    # 使用方法
    p = MyScatter(df, x='mpg', y='hp', title="HP vs MPG",
                  marker=marker('hp', markers=['circle']),
                  xlabel="Miles Per Gallon", ylabel="Horsepower")

    output_file("scatter.html")

    show(p)
